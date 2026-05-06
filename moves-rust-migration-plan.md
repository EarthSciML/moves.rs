# MOVES → Rust migration plan

A task-by-task plan for porting EPA's MOVES emissions model from Java/SQL/Go/Fortran to Rust, targeting the research and policy community rather than EPA regulatory use. Each task is sized to roughly one week of focused effort by a competent developer familiar with the relevant subsystem; tasks involving unfamiliar territory (Fortran semantics, MariaDB-specific SQL behavior) may run long.

## Design constraints driving this plan

- **Correctness first**: every behavioral change must be detected by an automated characterization test before merging.
- **No filesystem-mediated intermediate state**: all per-bundle scratch tables that currently live in `MOVESTemporary/`, `WorkerFolder/`, or in MariaDB's worker scratch databases are replaced by in-memory Polars `DataFrame` values passed between calculator stages.
- **Single-process execution**: the master/worker split is removed. Parallelism comes from Rayon-style data parallelism over the calculator graph, not from filesystem-mediated bundle handoff between OS processes.
- **All calculators rewritten in Rust** (Phase 3 Option B): no embedded SQL engine in the runtime path. Original SQL files retained as documentation and as the canonical reference for characterization tests.
- **Full NONROAD port to Rust**: the 29k-line Fortran NONROAD2008a model is rewritten in Rust rather than wrapped via FFI. Numerical fidelity risk is managed by structural choices outlined in Phase 5.
- **Control strategies preserved**: AVFT, Rate-of-Progress, and OnRoadRetrofit infrastructure carries over to the Rust port with the same input/output semantics.
- **Output format is Parquet** (no MariaDB dump converter): downstream consumers use pandas/R/Polars/DuckDB/Spark directly.
- **GUI is dropped**: CLI plus TOML-based RunSpec format alongside the legacy XML reader.
- **Distributed-worker infrastructure is dropped**: `amazon/`, multi-machine bundle protocols, the heartbeat thread.
- **Memory pressure is managed by bounded parallelism, not by spilling to disk**: see "Concurrency and memory model" below.
- **WebAssembly target includes NONROAD**: the Rust NONROAD port runs in the browser alongside the onroad calculators.

## Concurrency and memory model

Memory pressure in this architecture is dominated by how many `DataFrame` intermediates are live simultaneously, which scales linearly with the parallel-task width. The earlier draft of this plan reached for a disk-spill mechanism; that was the wrong instinct. The correct lever is bounded parallelism — a configurable concurrency limit that caps the number of simultaneously-active calculator chains.

Concretely: the Rust runtime maintains a worker pool sized by `--max-parallel-chunks N` (default: number of physical cores). Independent calculator chains from the calculator-graph DAG are dispatched to this pool; only N are running concurrently. Within a chain, calculators run sequentially and intermediate `DataFrame`s are dropped as soon as downstream calculators consume them. Peak memory is bounded by `N × max(chain_working_set)`. Users tuning for a memory-constrained environment lower N; users on a fat machine raise it. The MOVES Java code already has the analogous concept (`WorkerConfiguration.concurrentStatements`), so this is the same lever exposed in a more-direct way.

This approach has a concrete advantage over disk spill: it never induces I/O. Disk spill turns a memory-bound workload into a disk-bound one, which is strictly worse than waiting for a chunk to complete. We only fall back to disk-resident intermediates if a single chunk's working set exceeds available memory — which would be a rare case at County or Project scale and is something we'd debug as a calculator-level issue (likely a missing predicate pushdown), not paper over with an automatic spill.

Default-DB Parquet files are mmapped or memory-resident for fast reuse across chunks; that's a separate storage tier from the per-chunk intermediates and isn't affected by the parallelism limit.

## Calendar overview

| Phase | Tasks | Approximate weeks |
|-------|-------|------|
| 0 — Characterization-test harness | 1–7 | 7 |
| 1 — Coverage mapping | 8–10 | 3 |
| 2 — Framework port | 11–28 | 18 |
| 3 — Calculator port (Option B, full Rust rewrite) | 29–78 | 38 |
| 4 — Data layer | 79–90 | 12 |
| 5 — NONROAD port (full Rust rewrite) | 91–118 | 28 |
| 6 — Control strategies | 119–125 | 7 |
| 7 — Integration, hardening, release | 126–140 | 15 |
| **Total** | **140 tasks** | **~128 weeks (~30 months for one engineer)** |

A team of two to three engineers with effective parallelization across phases 2/4, 3/5, and 3/6 can collapse this to roughly 15–18 calendar months. Phase 0 is the critical path — it must be completed before the other phases begin meaningful work. Phase 5 (NONROAD) has internal dependencies but is otherwise independent of the onroad work and can run in parallel.

(Note: phase task numbers are non-contiguous because the Phase 5 expansion required adding tasks; the total count is 128 weeks of effort across 140 numbered task slots.)

---

## Phase 0 — Characterization-test harness

This phase produces the regression infrastructure that every other phase depends on. The deliverable is a test runner that takes a RunSpec, runs canonical MOVES, captures all intermediate and final state, and produces a deterministic snapshot artifact. Without this, the port is unverifiable.

### Task 1: Stand up canonical MOVES in a reproducible environment

Build a Docker image containing MariaDB 11.4 with the MOVES-required `my.cnf` configuration, JDK 17, Go 1.21, the MOVES source at a pinned commit, and the default database loaded from `database/Setup/movesdb20241112.zip`. Verify a clean run of `ant crun -Drunspec=testdata/SampleRunSpec.xml` produces output. This image is the reference implementation for the entire migration; pin it by digest.

### Task 2: Snapshot-format design and implementation

Define the canonical snapshot format for fixture outputs. Tables are exported as Parquet with row-stable sort orders (lexicographic on all columns of the natural key), numeric columns rounded to a defined precision (likely 1e-12 absolute tolerance, captured as fixed-decimal strings to eliminate float-formatting drift). Metadata sidecar captures schema, row count, and content hash. Build a Rust crate `moves-snapshot` that produces and compares snapshots.

### Task 3: Patch MOVES to capture intermediate state

Modify three flags in the canonical MOVES source: `OutputProcessor.keepDebugData = true`, `RemoteEmissionsCalculator.isTest = true`, `Generator.KEEP_EXTERNAL_GENERATOR_FILES = true`. Build a custom MOVES JAR that always runs in this mode for fixture runs. Verify intermediate captures land in `MOVESTemporary/`, `WorkerFolder/WorkerTempXX/`, and persist after the run. **Also** patch a parallel build of the standalone NONROAD Fortran to emit intermediate population/growth/age/emissions arrays (these become Phase 5's regression baseline).

### Task 4: Fixture-run automation and per-RunSpec snapshot capture

Wrap the patched MOVES in a runner script that takes a RunSpec path, executes against the Docker image, captures the output database, the execution database, all generator outputs, and all worker bundles. Each capture is normalized to the snapshot format from Task 2 and written to `characterization/snapshots/<fixture-name>/`. The script is deterministic given the same inputs.

### Task 5: Initial fixture set — the existing `SampleRunSpec.xml` plus expansion

Take `testdata/SampleRunSpec.xml` (single county, single hour, gasoline passenger cars, energy consumption pollutants) as fixture #1. Author six additional fixtures expanding one dimension at a time: full day instead of single hour; multiple months; multiple counties; different fuel type; different source type; criteria pollutants instead of energy. Each fixture must run in under five minutes against canonical MOVES.

### Task 6: Process and chain coverage fixtures

Author additional fixtures that systematically exercise each `EmissionProcess` (running, start, extended idle, brakewear, tirewear, evap permeation, evap fuel vapor venting, evap fuel leaks, refueling displacement, refueling spillage, crankcase variants, auxiliary power), each scale (Default, County, Project), each calculator chain endpoint from `CalculatorInfo.txt`, plus 10 NONROAD-specific fixtures spanning equipment categories and geography levels (county/state/national/subcounty/US-total). The NONROAD canonical reference is the **Windows-compiled** `NONROAD.exe` — not Linux gfortran, per the upstream-acknowledged compiler issue. Target: 30–35 fixtures total. Some will be slow (several minutes each); that's acceptable — full suite runs nightly, not per-commit.

### Task 7: Diff harness and CI integration

Build the comparison tool: takes two snapshot directories, produces a structured diff at table-row-cell granularity, with configurable per-column tolerance. Set up CI (GitHub Actions) to run the fixture suite against a pinned canonical-MOVES image weekly, alerting on any change. This gives us a regression detector for upstream MOVES updates as a side benefit.

---

## Phase 1 — Coverage mapping

Before porting, map which calculators each fixture actually exercises. The output of this phase is the migration order for Phase 3.

### Task 8: Static instrumentation of MOVES execution

Patch the canonical MOVES JAR to log every Java class instantiation in the calculator and generator hierarchies, every SQL file consumed (via the macro-expansion path through `SQLMacroExpander`), every Go calculator invoked. Build a structured execution trace per fixture. The `worker.sql` file MOVES already produces is most of this for free.

### Task 9: Coverage analysis and migration ordering

Aggregate execution traces across the fixture suite. Produce a coverage map: for each (Java class, SQL file, Go file), how many fixtures invoke it and what fraction of total execution time it accounts for. Hot-path candidates expected: `BaseRateCalculator.sql` (1.6k lines), `BaseRateGenerator.go` (2.4k lines), `CriteriaRunningCalculator.sql` (1.2k lines), `HCSpeciationCalculator.sql` (1.0k lines), `ActivityCalculator.sql` (1.0k lines), `OperatingModeDistributionGenerator`, `TotalActivityGenerator`. Output a ranked list driving Phase 3 task ordering.

### Task 10: Calculator chain reconstruction

Parse `CalculatorInfo.txt`'s 1,246 `Registration`/`Subscribe`/`Chain` directives plus the priority fields in `Subscribe` declarations plus the granularity declarations from each calculator's Java class. Reconstruct the actual execution DAG: for each (process, pollutant) pair, the ordered list of calculators that fire and their data dependencies. Write this as a machine-readable JSON document under `characterization/`. The DAG drives the calculator-chain implementation in Phase 2.

---

## Phase 2 — Framework port

Port the orchestration layer to Rust. This phase ends with a Rust binary that can parse a RunSpec, build the execution plan, and walk the calculator graph in the correct order — but executes no calculations (those come in Phase 3). The framework layer has reasonable existing test coverage (~35 of the 90 test files), which we port and run alongside the new Rust tests.

### Task 11: Rust workspace and project skeleton

Create a Cargo workspace with crates for `moves-runspec`, `moves-framework`, `moves-calculators`, `moves-data`, `moves-cli`, `moves-nonroad`. Set up CI (cargo test, clippy, rustfmt, deny). Establish coding conventions: error handling via `thiserror`, async via `tokio` only at I/O boundaries, dataframes via `polars` with `lazy` evaluation by default, parallelism via `rayon` with bounded thread pools. Document conventions in `CONTRIBUTING.md`.

### Task 12: RunSpec XML parser

Port `gov/epa/otaq/moves/master/runspec/` (5k lines, 23 Java files) to Rust using `quick-xml` and `serde`. Cover all RunSpec element types: geographic selections, time spans, vehicle selections, pollutant-process associations, output specifications, control strategy declarations. Validate against the existing 10 `RunSpecTest`/`RunSpecXMLTest` Java tests by porting them as Rust integration tests. RunSpec round-trip (parse → serialize → parse) must be byte-identical.

### Task 13: TOML-based RunSpec format

Design a TOML schema isomorphic to the XML RunSpec but human-friendlier (short tag names, named-enum values instead of numeric keys, comments-allowed). Write bidirectional converters. Document the mapping. This becomes the recommended format for the Rust port; XML is supported for compatibility with existing fixtures.

### Task 14: Pollutant, process, source-type, road-type definitional code

Port the static enumerations and definitional classes (`Pollutant.java`, `EmissionProcess.java`, `SourceType.java`, `RoadType.java`, `PollutantProcessAssociation.java` and their tests). These are mostly enum-shaped data with lookup tables; in Rust they become enums + `phf` (perfect-hash function) static maps. Port the existing tests; they cover the lookups thoroughly.

### Task 15: ExecutionRunSpec — the runtime view of a RunSpec

Port `ExecutionRunSpec.java` (2.4k lines). This is the runtime data structure that tracks: which iterations remain, which processes are active for the current iteration, the set of execution locations (county × zone × link triples), the active pollutant-process pairs, the output database connection. Major surgery: the existing class threads MariaDB `Connection` objects through; the Rust version threads `&mut DataFrameStore` instead.

### Task 16: ExecutionLocation and the location iterator

Port `ExecutionLocation.java` and `ExecutionLocationProducer.java`. These produce the sequence of (state, county, zone, link) tuples the master loop iterates over. The producer reads the active geographic selections from the RunSpec and the geography tables from the default database. Existing tests cover this well.

### Task 17: MasterLoopGranularity and subscription model

Port `MasterLoopGranularity.java` (the enum: PROCESS, YEAR, MONTH, DAY, HOUR, STATE, COUNTY, ZONE, LINK, MATCH_FINEST), `MasterLoopableSubscription.java` (the registration record), and the `MasterLoopable` trait. The compareTo logic that drives subscription ordering matters and must match Java exactly — the existing `MasterLoopableSubscriptionTest` is the regression check.

### Task 18: Calculator and Generator base traits

Define the Rust traits that replace `EmissionCalculator.java`, `Generator.java`, `GenericCalculatorBase.java`. Each calculator declares: its (process, pollutant) registration, its granularity, its priority, its upstream calculator chain, its input table dependencies, and an `execute(&self, ctx: &CalculatorContext) -> Result<DataFrame>` method. The trait is the contract Phase 3 calculators implement.

### Task 19: CalculatorRegistry — populated from CalculatorInfo.txt

Build the registry that loads `CalculatorInfo.txt` at startup, instantiates registered calculator structs (via a static lookup table), and constructs the calculator-chain DAG from Phase 1 Task 10's reconstruction. Provide methods for "all calculators relevant to this RunSpec" and "topological order for execution." The Java equivalent is `MOVESInstantiator.java` (1.9k lines).

### Task 20: MasterLoop core iteration

Port `MasterLoop.java` (1.0k lines) — the nested loop over iteration → process → location → year → month → day → hour. Each level fires `loopChange` notifications to subscribed calculators at matching granularity. Replace the Java `MOVESThread` synchronization with structured concurrency via `tokio` task scopes; the original parallelism is bundle-level and we don't need it. The existing `MasterLoopTest` is one method but useful as a smoke test.

### Task 21: Granularity-based loop notification

Port the `notifyLoopablesOfLoopChange` and `hasLoopables` logic from `MasterLoop.java` (lines 800–1000). This is the dispatch mechanism — at each loop level, iterate over subscribed calculators in priority order and call their `executeLoop` (now called `execute_at_granularity`) method. The priority ordering is subtle and must match Java; the priority comparator in `MasterLoopableSubscription.compareTo` is the reference.

### Task 22: SQL macro expander (kept as a documentation tool, not for runtime)

Port `SQLMacroExpander.java` (318 lines) and the associated section-marker processor. This is needed for two reasons: (1) Phase 3 calculator authors will reference the macro-expanded SQL when porting individual calculators, so we need to be able to produce it on demand; (2) the section-marker logic encodes RunSpec-conditional behavior that the Rust calculators must replicate. It does not run in the production code path.

### Task 23: ExecutionDatabaseSchema and CalculatorContext

Define the in-memory equivalent of MOVES's MariaDB execution database. The current MOVES execution database holds slowly-changing per-run data (filtered slices of the default DB) plus rapidly-changing per-bundle scratch tables. In Rust this becomes a `CalculatorContext` struct that owns: the per-run filtered default-DB tables (as Polars `DataFrame`s loaded once per run), a scratch namespace for inter-calculator hand-offs, and the current iteration/location/time triple. No filesystem, no MariaDB.

### Task 24: InputDataManager — the default-DB-to-execution-DB extraction

Port `InputDataManager.java` (4.7k lines, the largest file in `framework/`). This class reads the default database and constructs the per-run execution database by filtering tables to the RunSpec's active selections. In Rust, it reads Parquet files (Phase 4) lazily via Polars `LazyFrame`, applies pushdown predicates, and materializes into the `CalculatorContext`. Tests exist (`InputDataManagerTest`, 26 asserts).

### Task 25: AggregationSQLGenerator — postprocessing aggregation

Port `AggregationSQLGenerator.java` (1.9k lines). This builds the aggregation queries that take the worker output and roll it up to the user's requested output detail level (county/zone/link, year/month/day/hour, source-type, model-year, etc.). In Rust this becomes Polars `group_by`/`agg` expressions parameterized by the RunSpec's `outputemissionsbreakdownselection` and `outputtimestep`. The 5 `PreAgg*.sql` files are reference for the aggregation logic.

### Task 26: OutputProcessor

Port `OutputProcessor.java` (1.6k lines). This handles writing aggregated output to the output database. In Rust it writes Parquet files to the configured output directory, with one file per `MOVESOutput` table partition. The schema matches the MOVES output schema for downstream-tool compatibility.

### Task 27: MOVESEngine and the bounded-concurrency executor

Port `MOVESEngine.java` (2.0k lines) — the entry point that ties everything together: load RunSpec → instantiate ExecutionRunSpec → register calculators → build CalculatorContext → run MasterLoop → finalize OutputProcessor. The Rust version is much smaller because it doesn't manage a worker pool, doesn't manage MariaDB connections, and doesn't manage thread heartbeats.

**Includes implementing the bounded-concurrency executor**: a `rayon::ThreadPool` sized by `--max-parallel-chunks`, a `Semaphore` gating chunk dispatch, and the chunking logic that splits the calculator-graph DAG into independent chains. Each chain runs sequentially within itself; chains run concurrently up to the limit. Memory-pressure regression tests verify that doubling the parallelism limit roughly doubles peak RSS as expected.

### Task 28: CLI and end-to-end smoke test

Build `moves` CLI binary using `clap`. Subcommands: `run --runspec=<path> [--max-parallel-chunks=N]`, `import-cdb`, `convert-runspec` (XML↔TOML). End-to-end test: load `SampleRunSpec.xml`, walk the calculator graph (no calculators implemented yet — they all return empty `DataFrame`s with correct schemas), produce empty-but-correctly-shaped output. This task closes Phase 2.

---

## Phase 3 — Calculator port (Option B: full Rust rewrite)

Each calculator gets a full Rust rewrite. The original SQL stays in `database/` as canonical reference; the Rust implementation must produce numerically identical results (within tolerance) on the characterization fixtures. Calculators are ported in Phase-1-coverage-map order, which means hot-path first.

The size estimate per calculator depends on its source-line count, but most fall in the 0.5–1.5 week range. A few large ones (BaseRateCalculator, MultidayTankVaporVentingCalculator) need two weeks each.

### Generators (run before calculators in the master loop)

### Task 29: SourceBinDistributionGenerator

Apportions source-bin distributions across model year, fuel type, and regulatory class. Reads from `SourceUseTypePopulation` and related tables; writes the per-bundle source-bin allocation used by all running-emission calculators. ~600 lines of Java + ~200 lines of SQL.

### Task 30: OperatingModeDistributionGenerator (running)

Computes the distribution of vehicle operating modes (idle, cruise, acceleration, deceleration) by source type, road type, and average speed. Reads `DriveSchedule*` tables and `AvgSpeedDistribution`. The Java has a useful test (`OperatingModeDistributionGeneratorTest`).

### Task 31: AverageSpeedOperatingModeDistributionGenerator

Variant of Task 30 that uses average-speed-binned op-mode distributions instead of drive-schedule-derived ones; used when the RunSpec specifies `AvgSpeedDistribution` inputs. ~700 lines of Java.

### Task 32: StartOperatingModeDistributionGenerator

Op-mode distributions for engine starts (cold, warm, hot), keyed on soak time. Reads `StartTempAdjustment`, `SoakDistribution`. ~500 lines.

### Task 33: EvaporativeEmissionsOperatingModeDistributionGenerator

Op-mode distributions for evaporative processes (permeation, fuel-vapor venting, fuel leaks). Reads tank temperature profiles. ~600 lines.

### Task 34: LinkOperatingModeDistributionGenerator

Project-Scale-specific: op-mode distributions per link from user-supplied link drive schedules. ~400 lines.

### Task 35: MesoscaleLookupOperatingModeDistributionGenerator and MesoscaleLookupTotalActivityGenerator

Mesoscale-lookup variant for users running with `LookupOpModeDistribution`. ~500 lines combined.

### Task 36: TotalActivityGenerator

Computes total VMT, idle hours, and starts per (county, year, source type, fuel type). The single most important generator — every emission calculator's output is `rate × activity`, and this generator computes the activity. Reads VMT tables, age distributions, hotelling tables. ~1,200 lines of Java; existing test has only 1 assertion, so we author new characterization-test coverage. **Two weeks.**

### Task 37: SourceTypePhysics generator (Go-sourced)

Currently in `generators/sourcetypephysics/sourcetypephysics.go` (279 lines Go). Port logic: applies physics-based corrections (tractive power, vehicle-specific power) to source-type-keyed activity. Numerical fidelity matters here — the Go uses 64-bit floats and standard library math; ensure Rust uses identical operations.

### Task 38: TankTemperatureGenerator

Computes hourly tank-temperature trajectories from county meteorology and trip patterns. Used by evaporative calculators. Reads `ZoneMonthHour`, `Vehicle`, soak-distribution tables. ~700 lines.

### Task 39: TankFuelGenerator

Computes per-source-type, per-month tank fuel mass and Reid Vapor Pressure trajectories. ~500 lines.

### Task 40: FuelEffectsGenerator

Apportions emission rates across the fuel-formulation distribution, applying fuel adjustments via `generalFuelRatioExpression` table entries. ~1,500 lines of Java; has a real test (`FuelEffectsGeneratorTest`, 27 asserts) — port that as the regression baseline. **Two weeks.**

### Task 41: MeteorologyGenerator

Joins county-month-hour temperature and humidity profiles for the active execution locations. Reads `ZoneMonthHour` and the meteorology import tables. ~400 lines.

### Task 42: BaseRateGenerator (the Go-sourced one)

Currently in `generators/baserategenerator/baserategenerator.go` (2,391 lines Go). This is one of the two largest pieces of Go code in the system. Builds `BaseRateByAge` and `BaseRate` tables that the corresponding `BaseRateCalculator` consumes. Numerical fidelity is critical — this generator's output is the input to every running-emission calculation. **Two and a half weeks** including extensive characterization-test validation.

### Task 43: RatesOperatingModeDistributionGenerator

Used in rates-mode runs (`DO_RATES_FIRST` path) to produce op-mode distributions appropriate for emission-rate output rather than inventory output. ~500 lines.

### Generator validation milestone

### Task 44: Generator integration validation

End-to-end test: run all fixtures in the characterization suite through the Rust generators (Tasks 29–43), with calculators still stubbed. Compare generator outputs against canonical-MOVES intermediate captures from Phase 0. Expected divergences are tracked with explicit tolerance budgets; unexpected divergences are bugs to fix before proceeding.

### Calculators — exhaust running and start

### Task 45: BaseRateCalculator (Go-sourced)

Currently in `calc/baseratecalculator/baseratecalculator.go` (1,694 lines Go) plus `database/BaseRateCalculator.sql` (1,649 lines SQL). The calculator that produces base emission rates that all chained calculators consume. The largest single calculator port. **Three weeks** including correctness validation.

### Task 46: CriteriaRunningCalculator

The criteria-pollutant (NOx, CO, THC, PM) running-exhaust calculator. `database/CriteriaRunningCalculator.sql` (1,178 lines). Reads from `BaseRateOutput` (Task 45's output). **Two weeks.**

### Task 47: CriteriaStartCalculator

Criteria-pollutant start-exhaust calculator. `database/CriteriaStartCalculator.sql` (782 lines). Similar pattern to running.

### Task 48: HCSpeciationCalculator

Speciates total hydrocarbon output into methane, NMOG, NMHC, TOG, VOC. `database/HCSpeciationCalculator.sql` (981 lines), `calc/hcspeciation/` Go code. Reads `HCSpeciation` lookup tables. **Two weeks** combining SQL and Go-port effort.

### Task 49: NRHCSpeciationCalculator

Nonroad equivalent of Task 48. `database/NRHCSpeciationCalculator.sql`, `calc/nrhcspeciation/` Go code.

### Task 50: AirToxicsCalculator

Calculates benzene, formaldehyde, 1,3-butadiene, acetaldehyde, acrolein, naphthalene, ethyl benzene, hexane, propionaldehyde, styrene, toluene, xylene, MTBE, ethanol, 2,2,4-trimethylpentane from speciated HC. Plus the metallic toxics (mercury, arsenic, chromium, manganese, nickel) and dioxins/furans. `database/AirToxicsCalculator.sql` (749 lines), `calc/airtoxics/airtoxics.go` (448 lines). **Two weeks.**

### Task 51: AirToxicsDistanceCalculator

Distance-based variant of Task 50 used in rates-mode. ~400 lines combined.

### Task 52: NRAirToxicsCalculator

Nonroad equivalent. `calc/nrairtoxics/` plus SQL.

### Task 53: PMTotalExhaustCalculator and BasicRunningPMEmissionCalculator

Total exhaust PM and the running-exhaust PM components (elemental carbon, organic carbon, sulfate, nitrate). Combined ~600 lines.

### Task 54: BasicStartPMEmissionCalculator

Start-exhaust PM. ~400 lines.

### Task 55: PM10EmissionCalculator and PM10BrakeTireCalculator

PM10 calculators including brakewear and tirewear PM10 contributions. Combined ~500 lines.

### Task 56: BasicBrakeWearPMEmissionCalculator and BasicTireWearPMEmissionCalculator

Brakewear and tirewear PM2.5 calculators. Combined ~400 lines.

### Task 57: SulfatePMCalculator

Sulfate fraction of PM emissions, dependent on fuel sulfur level. ~400 lines.

### Calculators — non-running processes

### Task 58: EvaporativePermeationCalculator

Evaporative permeation through fuel system materials. `database/EvaporativePermeationCalculator.sql`, ~600 lines.

### Task 59: TankVaporVentingCalculator

Hot-soak and diurnal vapor venting. `database/TankVaporVentingCalculator.sql` (740 lines).

### Task 60: MultidayTankVaporVentingCalculator

Multi-day diurnal venting cycle calculator. `database/MultidayTankVaporVentingCalculator.sql` (2,066 lines — the largest single calculator SQL file). **Two weeks.**

### Task 61: LiquidLeakingCalculator

Evaporative fuel leaks. ~300 lines.

### Task 62: RefuelingLossCalculator

Refueling displacement vapor and spillage losses. ~500 lines.

### Task 63: CrankcaseEmissionCalculator and CrankcaseEmissionCalculatorNonPM and CrankcaseEmissionCalculatorPM

Three variants for crankcase emissions across running, start, and extended-idle processes. Combined ~700 lines.

### Calculators — GHG and special pollutants

### Task 64: CO2AERunningStartExtendedIdleCalculator

Atmospheric CO2 and CO2-equivalent for running, start, and extended-idle exhaust. `database/CO2AERunningStartExtendedIdleCalculator.sql`. ~400 lines.

### Task 65: CH4N2ORunningStartCalculator

Methane and nitrous oxide for running and start exhaust. ~400 lines.

### Task 66: NH3RunningCalculator and NH3StartCalculator

Ammonia calculators. Combined ~300 lines.

### Task 67: SO2Calculator

Sulfur dioxide based on fuel sulfur content. ~200 lines.

### Task 68: NOCalculator and NO2Calculator

NO and NO2 fractions of total NOx output. Combined ~300 lines.

### Task 69: WellToPumpProcessor and CO2AtmosphericWTPCalculator and CH4N2OWTPCalculator and CO2EqivalentWTPCalculator

Well-to-pump (upstream) emissions for energy-content pollutants. Combined ~500 lines.

### Task 70: TOGSpeciationCalculator

Total organic gas speciation into individual hydrocarbon species (separate from the HC speciation in Task 48 which only covers the gross categories). ~400 lines.

### Task 71: ActivityCalculator

Outputs activity tables (VMT, source hours, starts, etc.) to the output database alongside emissions, when the RunSpec requests activity output. `database/ActivityCalculator.sql` (964 lines). **Two weeks.**

### Task 72: DistanceCalculator

Distance output for inventory mode. ~300 lines.

### Calculator validation milestone

### Task 73: Calculator integration validation, hot path

End-to-end characterization-test pass: run all fixtures from Phase 0 against the Rust port through Tasks 45–72. Every calculator output table must match canonical-MOVES output within the defined tolerance. Triage divergences: bugs in the port, intentional improvements (e.g. fixing known MOVES issues), or numerical artifacts. Document the budget for tolerated artifacts.

### Task 74: Calculator integration validation, full coverage

Same as Task 73 but covering the full fixture suite, including fixtures that exercise non-hot-path calculators (rare process-pollutant combinations). Identifies any calculator we missed in the port plus any cross-calculator interaction we got wrong. Likely produces a punch-list that consumes part of Phase 7.

### Phase 3 milestones

### Task 75: Performance baseline

Profile the Rust port on representative fixtures: per-calculator wall time, peak memory, cache miss rate. Compare against canonical MOVES on the same inputs. Expectation is a 5–20× improvement on identical hardware just from removing MariaDB-MyISAM and the bundle-handoff overhead; if we're not seeing that, find the bottleneck.

### Task 76: Concurrency tuning and memory-pressure validation

Sweep `--max-parallel-chunks` from 1 to NCPU on representative workloads, measuring throughput and peak RSS. The expected curve is roughly linear throughput up to a memory or memory-bandwidth limit, then flat. Validate that peak memory tracks `N × max_chain_working_set` as the model predicts; if it doesn't, calculator chains are sharing more state than they should and we have a correctness bug masquerading as a memory issue. Document recommended `N` values for common machine sizes (laptop: 2–4, workstation: 8–16, server: 32+).

### Task 77: Calculator concurrency correctness

Run the full characterization suite at multiple `--max-parallel-chunks` values (1, 4, NCPU) and verify byte-identical outputs across all settings. Any divergence indicates a calculator that is not pure with respect to its inputs — typically a hidden dependency on iteration order, a shared mutable state, or non-deterministic floating-point summation across threads. These bugs are easier to find now than later.

### Task 78: Phase 3 closing checkpoint

Phase 3 review: every calculator from `CalculatorInfo.txt` is implemented in Rust, the characterization suite passes within tolerance budget, performance meets target, the SQL files are no longer on the runtime path. The remaining SQL files live in `reference/` as documentation.

---

## Phase 4 — Data layer

In parallel with later Phase 3 tasks. Converts the MOVES default database from a 390MB MariaDB dump to partitioned Parquet, builds importers for user input databases, and defines the output Parquet schema.

### Task 79: Default database schema audit

Inventory every table in `movesdb20241112.sql`: row count, primary key, columns most-frequently-filtered-on (from Phase 1 coverage map), update frequency. Output a partitioning plan: which tables stay as single Parquet files, which partition by year, which partition by county, which partition by both. Most lookup tables are small enough (<10k rows) to remain monolithic; the large ones (`SHO`, `SourceHours`, `EmissionRate*`) need partitioning.

### Task 80: Default database conversion pipeline

Build a reproducible conversion script: takes a MariaDB dump file, loads it into a temporary MariaDB instance, exports each table to Parquet according to the partitioning plan, validates row counts, computes content hashes, writes to a versioned directory layout (`default-db/movesdb20241112/<table>/...`). Re-runnable for future EPA default-DB releases.

### Task 81: Convert and validate the current default database

Run the pipeline from Task 80 on `movesdb20241112.zip`. Validate: every row from the source SQL dump appears exactly once in the Parquet output; every column type round-trips correctly (especially `DOUBLE` precision and `DATE`/`DATETIME` timezone handling); reading-back via Polars produces identical aggregates to MariaDB. Any discrepancy is a blocker.

### Task 82: Lazy-loading default-DB reader

Build the Rust crate `moves-data-default` that exposes the default DB as a typed Rust API: `DefaultDb::source_use_type_population(filter: SourceUseTypeFilter) -> Result<DataFrame>`. Internally uses Polars `LazyFrame` with predicate pushdown into Parquet. For the partitioned tables, only the relevant partitions are loaded. Wire into the Phase 2 `InputDataManager`.

### Task 83: County Database (CDB) importer

Port the County-scale input database importer from `gov/epa/otaq/moves/master/implementation/importers/` (88 Java files; the County importers are ~12k lines). User-facing input: Excel/CSV files following the CDB template. Output: Parquet tables matching the default-DB schema. Validates: column types, value ranges, allocation-table summation invariants (the ones `DebuggingMOVES.md` warns about). **Two weeks.**

### Task 84: Project Database (PDB) importer

Port the Project-scale importer (link drive schedules, link source types, off-network idling, etc.). Distinct from the CDB importer because the Project Scale uses different input tables. ~6k lines of Java equivalent. **Two weeks.**

### Task 85: Nonroad input database importer

Port the Nonroad input importers — population, age distribution, retrofit, monthly throttle. Smaller than CDB/PDB but with different schema. ~4k lines.

### Task 86: AVFT input importer (and AVFT Tool equivalent)

Alternative Vehicle Fuel Technology input database. Allows users to specify custom electric-vehicle and alternative-fuel adoption trajectories. Has its own GUI in the Java original; we provide a CLI equivalent that takes a TOML/CSV input.

### Task 87: LEV/NLEV input importer

Low-Emission-Vehicle and National-LEV alternative-rate inputs. Smaller scope, ~1k Java equivalent.

### Task 88: Importer validation suite

Author characterization tests for the importers: take each fixture from Phase 0 that uses a CDB/PDB, run the Rust importer against the user-supplied source files, compare the resulting Parquet to canonical-MOVES's loaded MariaDB tables. Differences indicate importer bugs.

### Task 89: Output database Parquet writer

Define and document the output schema: `MOVESOutput.parquet`, `MOVESActivityOutput.parquet`, `MOVESRun.parquet` (run metadata), with partitioning by year and month. Schema matches the legacy MOVES output schema for the columns that exist in both; new columns (run hash, calculator-version, etc.) are additive. Wire into Phase 2 `OutputProcessor`.

### Task 90: Output documentation and downstream-tool examples

Write user-facing documentation: how to load Rust-MOVES output into pandas, R, Polars, DuckDB, Spark. Include three example analyses showing canonical-MOVES-equivalent post-processing (NEI submission summaries, county-level inventory rollups, rates-mode CSV exports). Goal: zero friction for downstream researchers.

---

## Phase 5 — NONROAD port (full Rust rewrite)

The 29k-line Fortran NONROAD2008a model in `NONROAD/NR08a/SOURCE/`. This phase is substantially expanded from prior versions of this plan, based on a direct audit of the Fortran source.

### What's actually there

The codebase consists of 118 `.f` files totaling 29,361 lines, plus 11 `.inc` include files (2,433 lines) defining 65 named COMMON blocks that hold all global state.

**Architectural shape:**

- **Static dimensioning everywhere.** All arrays use Fortran-77-style fixed-size declarations driven by parameters in `nonrdprm.inc`: `MXEQIP=25` equipment categories, `MXPOL=23` pollutants, `NSTATE=53`, `NCNTY=3400`, `MXTECH=15` technology types, `MXEVTECH=15` evap technology types, `MXHPC=18` horsepower categories, `MXAGYR=51` model-year ages, `MXDAYS=365`, `MXSUBC=300`, `MXEMFC=13000`, `MXDTFC=120`, `MXPOP=1000`. The Rust port replaces these with `Vec` and `ndarray` types of dynamic size, removing several MOVES-side capacity workarounds in the process.
- **No subprocesses, no databases, no threading.** NONROAD is a single-threaded batch program: read options file → read all reference data into COMMON-block arrays → loop over (geography × equipment × year) records computing emissions → write output records. The simplicity is real and helps the port.
- **Six near-duplicate process routines.** `prccty.f` (790 lines), `prcsta.f` (1034 lines), `prcsub.f` (829 lines), `prcus.f` (775 lines), `prc1st.f` (785 lines), `prcnat.f` (943 lines) handle different geography levels (county, state-to-county, subcounty, US-total, state-from-national, national). They share substantial structure — `prccty` and `prcsta` differ in roughly 550 non-comment lines — and a careful Rust port can collapse them into a single parameterized routine. Doing so is worth a week of effort and removes ~3,000 lines of duplication.
- **Fixed-width ASCII I/O.** Input files (`.POP`, `.ALO`, `.GRW`, `.EMF`, `.DAT`, etc.) are column-precise text formats. Field positions are documented in the file headers and read via Fortran `READ(unit, format)` statements.
- **Subprocess invocation from MOVES.** MOVES does *not* link NONROAD as a library — it generates a configuration `.opt` file and all required input data files, ships them as a worker bundle, and invokes `nonroad.exe` as a subprocess. The Java↔Fortran integration is in `gov/epa/otaq/moves/master/nonroad/` (15 files, 6.6k lines for input generation) and `gov/epa/otaq/moves/worker/framework/Nonroad{OutputDataLoader,PostProcessor}.java` (~2k lines for output ingestion). This is good news: the existing integration is already shaped like a function call, so swapping the subprocess invocation for a direct Rust function call is a clean substitution.
- **Known gfortran issue in the upstream.** The `NONROAD/NR08a/SOURCE/readme.md` warns that gfortran on some Linux platforms produces "incorrectly near-zero emission results for some equipment types" — a reminder that compiler-Fortran semantic interactions are real and the Rust port must validate against the Windows-compiled reference, not against a Linux gfortran build.

**Functional grouping of the 118 source files** (based on the makefile's include-file dependencies and inspection of each routine):

1. **Main driver and process loop** (4 files, ~3,500 lines): `nonroad.f` (main entry point, 397 lines), `dayloop.f`, `daymthf.f`, `dispit.f`. Drives the SCC × geography × year iteration.
2. **Geography processing** (6 files, ~5,156 lines): `prccty.f`, `prcsta.f`, `prcsub.f`, `prcus.f`, `prc1st.f`, `prcnat.f`. The bulk of the spatial-allocation logic.
3. **Population, growth, age** (5 files, ~1,400 lines): `getpop.f`, `getgrw.f`, `grwfac.f`, `agedist.f`, `modyr.f`. Population apportionment, growth-factor application, age-distribution and model-year fraction computation.
4. **Emission factor lookup and calculation** (10 files, ~3,000 lines): `clcems.f` (exhaust), `clcevems.f` (evaporative, 721 lines — the largest single file), `emfclc.f`, `evemfclc.f`, `emsadj.f`, `clcrtrft.f` (retrofit), `unitcf.f`, `intadj.f`, plus retrofit validators.
5. **Allocation and spatial apportionment** (3 files, ~530 lines): `alocty.f`, `alosta.f`, `alosub.f`.
6. **Input file parsers** (~30 `rd*.f` files, ~7,000 lines): `rdpop.f`, `rdalo.f`, `rdgrow.f`, `rdemfc.f`, `rdevemfc.f`, `rdtech.f`, `rdtech_moves.f`, `rdevtech.f`, `rdevtech_moves.f`, `rdseas.f`, `rdspil.f`, `rdsulf.f`, `rdrtrft.f`, `rdact.f`, `rddetr.f`, `rdfips.f`, `rdind.f`, `rdnropt.f`, `rdnrper.f`, `rdnrreg.f`, `rdnrsrc.f`, `rdrgndf.f`, `rdscrp.f`, `rdstg2.f`, `rdgxrf.f`, `rdalt.f`, `rdbsfc.f`, `rdday.f`, `rdefls.f`. One per input file format.
7. **Output writers and utilities** (~50 small files, ~6,000 lines): `wrt*.f` output routines (`wrtams.f`, `wrtbmy.f`, `wrtdat.f`, `wrthdr.f`, `wrtmsg.f`, `wrtsi.f`, `wrtsum.f`); `fnd*.f` lookup helpers (`fndchr.f`, `fndasc.f`, `fndhpc.f`, etc.); `chk*.f` validators; string utilities (`strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`, `chrsrt.f`); FIPS-code initializers `in1fip.f`–`in5fip.f`.

### Phase 5 task list

### Task 91: NONROAD architecture map and Rust crate skeleton

Produce a design document mapping each of the seven functional clusters to a Rust module: `moves-nonroad::main` (driver loop), `moves-nonroad::geography` (process routines), `moves-nonroad::population` (population/growth/age), `moves-nonroad::emissions` (calculation), `moves-nonroad::allocation`, `moves-nonroad::input` (parsers), `moves-nonroad::output` (writers and utilities), `moves-nonroad::common` (replacement for COMMON blocks — typed structs holding what was global state). Establish: array-size policy (replace fixed `MXEQIP`-style limits with `Vec`, capture the original limits as documentation), error handling policy (Fortran's integer error returns become `Result`), I/O policy (idiomatic Rust `BufRead` and `Write` traits, not Fortran-style unit numbers).

Importantly, this design must be **WASM-compatible from day one**: no `std::process`, no platform-specific `std::os` calls, no Fortran FFI in the runtime path. Per Task 122, the WASM target rules out the Fortran-FFI escape hatch that an FFI-ready design might otherwise rely on. This raises the bar for Task 104 (numerical fidelity) but is a cleaner long-term position.

### Task 92: COMMON block replacement design

The 65 COMMON blocks across 11 include files are NONROAD's global state. They group into ten typed Rust structs corresponding to the 11 includes (one struct per include, with `nonrdprm.inc`'s parameters folded into `const` items). Each struct is owned by a top-level `NonroadContext` struct that gets passed explicitly between modules — replacing implicit global-via-COMMON with explicit parameter passing. This is a substantial design task because some COMMON blocks share variables and the dependencies must be carefully traced. **Two weeks.**

### Task 93: Parameter and constant translation

Port `nonrdprm.inc` (732 lines) to a Rust `consts` module: all `parameter` declarations become `pub const` items. Document units and provenance for each (the original `.inc` file has these as comments; preserve them). Separate constants that are true compile-time invariants (chemical constants like `DENGAS = 6.237`) from former array-dimension parameters (which become documentation only).

### Task 94: Input parsers — population (.POP) and allocation (.ALO)

Port `rdpop.f` (446 lines) and `rdalo.f` (278 lines). These are the two highest-volume input formats. Establish the parser pattern using `nom` or hand-written line-by-line parsers; both files use space-separated and column-aligned fields. Includes parsing the multi-record FIPS continuation format that several allocation files use.

### Task 95: Input parsers — growth (.GRW) and seasonal (.DAT)

Port `rdgrow.f` (382 lines), `rdgxrf.f` (169 lines), `rdseas.f` (280 lines), `rdday.f` (255 lines).

### Task 96: Input parsers — emission factors (.EMF) and technology (.TCH)

Port `rdemfc.f` (354 lines), `rdevemfc.f` (383 lines), `rdtech.f` (305 lines), `rdtech_moves.f` (298 lines), `rdevtech.f` (334 lines), `rdevtech_moves.f` (337 lines). These are the core emission rate inputs. **Two weeks.**

### Task 97: Input parsers — activity, deterioration, and miscellany

Port `rdact.f` (434 lines), `rddetr.f` (209 lines), `rdspil.f` (311 lines), `rdsulf.f` (220 lines), `rdrgndf.f` (200 lines), `rdscrp.f` (177 lines), `rdstg2.f` (136 lines), `rdalt.f` (202 lines), `rdbsfc.f` (109 lines), `rdefls.f` (228 lines), `rdfips.f` (238 lines), `rdind.f` (355 lines), `rdnropt.f` (438 lines), `rdnrper.f` (494 lines), `rdnrreg.f` (278 lines), `rdnrsrc.f` (191 lines). Most are short and follow the patterns established in Tasks 94–96. **Two weeks.**

### Task 98: Retrofit input parser

Port `rdrtrft.f` (710 lines) plus the retrofit validators `vldrtrftrecs.f` (432 lines), `vldrtrfthp.f`, `vldrtrftscc.f`, `vldrtrfttchtyp.f`. Retrofit input format is the most complex of the input files because it cross-references multiple other inputs.

### Task 99: Initialization and option-file processing

Port `iniasc.f` (653 lines), `opnnon.f` (633 lines), `opnefc.f` (437 lines), `intnon.f` (307 lines), `intadj.f` (141 lines), `intams.f` (134 lines), `getsys.f`, `getime.f`, `getind.f` (313 lines). These read the `.opt` configuration file and initialize the COMMON-block state.

### Task 100: FIPS code initializer

Port `in1fip.f`, `in2fip.f`, `in3fip.f`, `in4fip.f`, `in5fip.f` (~600 lines combined). These hard-code the FIPS code lookup tables. In Rust this becomes a `phf_map` or a static table.

### Task 101: Find/lookup utility routines

Port `fndchr.f`, `fndasc.f`, `fndact.f`, `fnddet.f`, `fndefc.f`, `fndevefc.f`, `fndevtch.f`, `fndgxf.f`, `fndhpc.f`, `fndkey.f`, `fndreg.f`, `fndrfm.f`, `fndrtrft.f`, `fndscrp.f`, `fndtch.f`, `fndtpm.f` (~2,500 lines combined). These are linear-search-with-fallback lookup helpers; in Rust most become indexed `HashMap` or `BTreeMap` lookups, which is faster than the original. **Two weeks.**

### Task 102: String utilities

Port `strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`, `chrsrt.f`, `wadeeq.f`, `cnthpcat.f` (~700 lines combined). Most of these become one- or two-line Rust functions using `&str` methods. Several have semantics specific to Fortran's blank-padded character variables; preserve those semantics or document deviations.

### Task 103: Population and growth core

Port `getpop.f` (285 lines), `getgrw.f` (200 lines), `grwfac.f` (281 lines), `getscrp.f` (107 lines), `cmprrtrft.f` (153 lines), `srtrtrft.f` (116 lines), `swaprtrft.f` (133 lines), `rtrftengovrlp.f` (110 lines), `initrtrft.f`. The population apportionment and growth-factor logic.

### Task 104: Age distribution and model year

Port `agedist.f` (193 lines) and `modyr.f` (216 lines). The age-distribution and model-year-fraction computation. Numerical fidelity matters here — the algorithm uses iterative or accumulating computations that are sensitive to evaluation order.

### Task 105: Allocation routines

Port `alocty.f` (181 lines), `alosta.f` (176 lines), `alosub.f` (170 lines). The county/subregion allocation logic.

### Task 106: Exhaust emissions calculator

Port `clcems.f` (360 lines), `emfclc.f` (314 lines), `emsadj.f` (343 lines), `unitcf.f` (80 lines). The core exhaust-emissions calculation. **Two weeks** including numerical-fidelity validation against canonical fixtures.

### Task 107: Evaporative emissions calculator

Port `clcevems.f` (721 lines, the largest single file), `evemfclc.f` (370 lines). The evaporative emissions calculation. **Two weeks.**

### Task 108: Retrofit emission calculator

Port `clcrtrft.f` (309 lines).

### Task 109: Geography processing — county and subcounty

Port `prccty.f` (790 lines) and `prcsub.f` (829 lines), the county-level and subcounty-level process routines. Design choice: port them as independent functions first, then refactor to a shared parameterized routine in Task 112. **Two weeks.**

### Task 110: Geography processing — state and state-from-national

Port `prcsta.f` (1,034 lines) and `prc1st.f` (785 lines). **Two weeks.**

### Task 111: Geography processing — national and US-total

Port `prcnat.f` (943 lines) and `prcus.f` (775 lines). **Two weeks.**

### Task 112: Geography processing refactor

Refactor Tasks 109–111 into a single parameterized routine, removing the substantial duplication. Validate with characterization fixtures that the refactor doesn't change outputs. This is structurally optional — the previous Tasks would already produce a working port — but it pays back maintenance cost for the lifetime of the project.

### Task 113: Main driver loop

Port `nonroad.f` (397 lines), `dayloop.f` (126 lines), `daymthf.f` (194 lines), `dispit.f` (50 lines), `mspinit.f`, `spinit.f`, `scrptime.f` (212 lines). The top-level orchestration that ties parsing, calculation, and writing together.

### Task 114: Output writers

Port `wrtams.f` (161 lines), `wrtbmy.f` (280 lines), `wrtdat.f` (249 lines), `wrthdr.f` (292 lines), `wrtmsg.f` (242 lines), `wrtsi.f` (102 lines), `wrtsum.f` (117 lines), `hdrbmy.f` (213 lines), `sitot.f` (140 lines), `chkasc.f`, `chkwrn.f`, `clsnon.f`, `blknon.f`. Fortran-format output writers; the Rust port emits two formats: the original NONROAD output for backwards compatibility, plus Parquet for native consumption.

### Task 115: NONROAD numerical fidelity validation

Run all Phase 0 NONROAD fixtures through the Rust port; diff against the Windows-compiled NONROAD reference output. Tolerance budget: 1e-9 relative for energy quantities, 1e-12 absolute for counts and indices, exact match for SCC/equipment/year keys. Numerical divergences will surface here — likely sources include `EXP`/`LOG`/`POW` differences between Fortran intrinsics and Rust's `libm`, summation-order differences in iterated population aggregation (Task 104's age-distribution computation is a known risk), and accumulated rounding in the 6-deep nested loops of the geography routines.

### Task 116: NONROAD numerical-divergence triage

Reserved for fixing whatever Task 115 surfaces. Probable interventions: replacing Rust's default `f64::exp` with a Fortran-compatible implementation for the routines where it diverges, switching summation order to match Fortran's left-to-right evaluation, using Kahan summation in specific accumulators that show drift.

**Note on the FFI fallback option:** in earlier drafts of this plan, "wrap the original Fortran via `cc-rs` + `gfortran` for routines that won't reach fidelity in pure Rust" was offered as a release-blocker mitigation. Per the WASM-compatibility constraint adopted in Task 91 and reinforced by Task 122, this fallback is **not available** in the runtime path — gfortran does not target wasm32. If a class of equipment shows persistent divergence beyond tolerance, the options are (a) keep working in pure Rust until fidelity is achieved (likely outcome for most divergences), (b) widen the documented tolerance for the affected pollutant/equipment combination and ship the divergence as a known difference, or (c) ship native and WASM with different fidelity guarantees, with native using Fortran FFI as an escape hatch. Option (c) creates a maintenance burden and is a last resort. **Two weeks** budgeted, possibly more if option (a) requires deep work.

### Task 117: NONROAD-MOVES integration

Replace the existing `gov/epa/otaq/moves/master/nonroad/` Java↔Fortran bridge (15 files, 6.6k lines for input generation) and `gov/epa/otaq/moves/worker/framework/Nonroad{OutputDataLoader,PostProcessor}.java` (~2k lines for output ingestion) with direct Rust function calls. The Rust orchestrator (Phase 2) calls into `moves-nonroad::run_simulation(opts: &NonroadOptions, inputs: &NonroadInputs) -> NonroadOutputs` directly; no subprocess, no scratch files, no MariaDB ingestion step. The Rust onroad/nonroad output schemas converge on the unified Parquet output from Phase 4 Task 89.

### Task 118: NONROAD-specific post-processing

Port the Nonroad post-processing scripts in `database/NonroadProcessingScripts/` (the SQL-based gram-per-hour-by-SCC summarization). These become Polars expressions in the Rust output processor. ~300 lines of SQL.

---

## Phase 6 — Control strategies

The control-strategy infrastructure modifies inputs that calculators consume. Three concrete strategies in MOVES: AVFT, Rate-of-Progress, and OnRoadRetrofit. Each has its own logic; the framework that connects them to the master loop is shared.

### Task 119: Control-strategy framework

Port `gov/epa/otaq/moves/master/framework/` control-strategy machinery: `InternalControlStrategy` interface, the registration mechanism, the lifecycle hooks (pre-run, per-iteration, post-run). The Rust version uses traits + dynamic dispatch via a registry; control strategies declare which input tables they modify.

### Task 120: AVFT control strategy

Alternative Vehicle Fuel Technology — modifies the `AVFT` input table to apply user-specified electric/alternative-fuel adoption. Currently in `gov/epa/otaq/moves/master/implementation/ghg/internalcontrolstrategies/avft/` (~1.5k lines Java). Includes the AVFT Tool's logic for building the AVFT table from user inputs.

### Task 121: Rate-of-Progress control strategy

Applies emission-reduction percentages by pollutant, source type, regulatory class, and model year — used to model the effect of new regulations. Currently in `internalcontrolstrategies/rateofprogress/` (~1k lines). Reads strategy parameters from RunSpec's `<internalcontrolstrategy>` block.

### Task 122: OnRoadRetrofit control strategy

Retrofit programs: percentage of fleet retrofitted with emission-control equipment, reducing emissions by a specified factor. `internalcontrolstrategies/onroadretrofit/` (~700 lines). Inputs: source type, model year range, retrofit fraction, retrofit effectiveness.

### Task 123: NONROAD retrofit integration

The Rust NONROAD port (Phase 5) has its own retrofit support via `clcrtrft.f`-derived code (Task 108). Wire the OnRoadRetrofit-equivalent input format for NONROAD into the unified control-strategy framework, so a single RunSpec retrofit declaration applies to both the onroad and nonroad calculators where applicable.

### Task 124: Control-strategy validation

Author fixtures exercising each control strategy with non-trivial parameters; capture canonical-MOVES output; validate Rust port matches. Cross-strategy interactions (multiple strategies active simultaneously) get explicit test cases — the Java order-of-application is the reference behavior.

### Task 125: Control-strategy documentation and Phase 6 closing checkpoint

User documentation: TOML schema for declaring control strategies in a RunSpec, examples of each strategy type, common pitfalls. Phase 6 review: all three control strategies plus the framework are implemented in Rust, the validation fixtures pass, NONROAD retrofits are unified with the onroad framework. Document any behavioral divergences from canonical MOVES.

---

## Phase 7 — Integration, hardening, release

### Task 126: Full-suite regression pass

Run every fixture in the characterization suite end-to-end against the complete Rust port. Triage every divergence. Many of these will be small: ordering differences in tied-row aggregates, sub-tolerance numerical drift, log-message format differences. A few will be real bugs to fix. Output: a "known divergences" document published with the release.

### Task 127: Performance benchmark suite

Build a public-facing benchmark report comparing the Rust port to canonical MOVES on representative workloads: County-Scale single-county-single-year, County-Scale multi-county-multi-year, Project-Scale, Default-Scale national, rates-mode, NONROAD-only, mixed onroad+nonroad. Per-workload metrics: wall time at multiple `--max-parallel-chunks` values, peak memory at each, output-correctness-vs-tolerance. Goal: 10–50× wall-time improvement for County and Project scales, similar for Default; NONROAD on a single thread should beat the Fortran by 2–5× thanks to better lookup data structures.

### Task 128: Documentation — user guide

User-facing documentation: installation (single static binary, no MariaDB, no JDK), getting-started tutorial using the existing `SampleRunSpec.xml`, full RunSpec reference (XML and TOML), CDB/PDB importer guide, output schema reference, downstream-tool examples (extending Task 90), guidance on tuning `--max-parallel-chunks` for memory-constrained environments.

### Task 129: Documentation — porting guide

For users with existing MOVES workflows: how to port a canonical-MOVES RunSpec to the Rust port, what behaviors differ, what's not yet supported, how to compare outputs. Explicitly call out the regulatory-validity caveat (this port is not for SIP submissions).

### Task 130: Documentation — developer guide

For contributors: architecture overview, calculator-implementation guide (how to add or modify a calculator with characterization tests), data-layer extension guide (how to add a new default-DB table), control-strategy authoring guide, NONROAD module guide.

### Task 131: Release packaging

Cross-platform binary releases (Linux x86_64, Linux aarch64, macOS x86_64, macOS aarch64, Windows x86_64). Default-database Parquet files hosted as a separate downloadable artifact. Reproducible builds via `cargo build --locked`.

### Task 132: WebAssembly target — onroad

Build the Rust port as a `wasm32-unknown-unknown` target with the orchestration plus onroad calculators. Default DB hosted as Parquet; user input via file upload + OPFS. Concurrency tuning for browser context: `--max-parallel-chunks` defaults to 1 in WASM until Task 134 enables threading; the bounded-concurrency executor still serves to limit peak memory in single-threaded browser execution by sequencing chunks rather than running them all at once.

### Task 133: WebAssembly target — NONROAD

Add the `moves-nonroad` crate to the WASM build. The pure-Rust NONROAD implementation ports to WASM cleanly; the input parsers and output writers need WASM-compatible I/O (browser file APIs via `wasm-bindgen` rather than `std::fs`). The numerical-fidelity work from Tasks 115–116 is doubly relevant here: WASM uses its own `libm` implementation and any divergence between native-Rust and WASM-Rust math must be characterized.

This task also surfaces the constraint that drove the design choice in Task 91 and Task 116: **the Fortran-FFI escape hatch is unavailable in WASM**, so any divergences that proved hard to resolve in pure Rust on native must be resolved here too. If Task 116 chose to ship native with FFI fallback for some equipment classes (option c in that task's description), this task is where the WASM divergence becomes visible — likely as a wider tolerance budget for those equipment classes in the WASM build, documented as a known difference. **Two weeks.**

### Task 134: WebAssembly multi-threading

Enable `wasm32-unknown-unknown` with the threads proposal (requires SharedArrayBuffer + cross-origin isolation). Configure `rayon` to use Web Workers as the thread pool. Re-enable `--max-parallel-chunks > 1` in WASM. Document the cross-origin-isolation deployment requirement (COEP/COOP headers) for hosts that want to enable this.

### Task 135: WebAssembly demo and documentation

Build a minimal browser-hosted demo that runs `SampleRunSpec.xml` end-to-end (onroad + nonroad), with inputs via file picker, outputs as downloadable Parquet, and a progress display. Documentation for embedding the WASM module in third-party tools. The point isn't a polished UI — it's a credible existence proof that the port runs in the browser.

### Task 136: Continuous-integration hardening

GitHub Actions matrix: build on all release platforms (including WASM), run `cargo test`, run the small-fixture characterization subset on PRs, run the full characterization suite weekly against an updated canonical-MOVES image, run the NONROAD characterization suite against a Windows-compiled NONROAD reference. Fail builds on any divergence beyond declared tolerance budget. Specifically test the chunked-parallelism behavior at multiple N values to catch concurrency regressions. Also runs the WASM build of the characterization subset, catching native-vs-WASM divergence.

### Task 137: Upstream-tracking workflow

Document and automate the process for incorporating EPA's annual MOVES updates: detect default-DB schema changes, regenerate Parquet files, re-run characterization suite against updated canonical MOVES, surface any new calculator behaviors, update Rust calculators if necessary, version-tag the release.

### Task 138: First public release

Tag v0.1.0. Publish release notes documenting: scope (what's ported), known divergences, performance results, regulatory caveat, contribution guidelines, browser demo URL. Announce to the relevant research and policy communities (transportation researchers, state air quality agencies' modeling teams, academic groups working on emissions).

### Task 139: Post-release triage and feedback intake

Reserved for post-release: fixing reported issues, prioritizing user-requested features. The product is not done at v0.1; this task acknowledges that.

### Task 140: v0.2 planning

Plan v0.2.0 scope based on v0.1 feedback. Likely candidates: incorporating the next EPA MOVES release, expanding RunSpec coverage to less-common configurations, performance optimization passes informed by real-world workloads.

---

## Risks and mitigations

**Numerical fidelity divergences.** Floating-point summation order, MariaDB vs Polars `SUM` precision, transcendental-function differences in Fortran→Rust, WASM-vs-native math. Mitigation: explicit per-pollutant tolerance budgets, characterization-test triage workflow, "known divergences" public document. Some divergences are unavoidable; the goal is to make them visible and bounded.

**NONROAD numerical fidelity is the highest-risk single area.** The 29k-line Fortran has 30+ years of accumulated numerical conventions, the upstream itself has documented compiler-dependent failures (the gfortran-on-Linux warning), and the WASM target rules out the FFI-fallback escape hatch that would otherwise be available on native. Mitigation: aggressive characterization testing against the Windows-compiled reference; willingness to spend extra time in Task 116 to chase down arithmetic divergences; willingness to accept slightly larger tolerance for nonroad than for onroad if specific divergences prove intractable. Worst case: NONROAD ships in v0.1 with a documented "for research/policy use; small numerical divergences from canonical NONROAD may exist" caveat, and tightening fidelity continues post-release.

**Calculator chain ordering subtleties.** The MOVES master loop's calculator dispatch depends on subtle priority and granularity ordering that isn't fully documented; Phase 1 Task 10 attempts to recover this but may miss edge cases. Mitigation: characterization tests at every chain endpoint; Task 77 explicitly tests for non-determinism.

**EPA's annual default-DB updates.** A new MOVES release lands roughly annually with updated rates and possibly schema changes. Mitigation: Task 137 codifies the upstream-tracking workflow; expect ~1 person-month per upstream release for incorporating changes.

**Memory pressure under wide parallelism.** Bounded-concurrency executor caps peak memory at `N × max_chain_working_set`, but if a single chain's working set grows unexpectedly large (say, a national-scale RunSpec with all pollutants), even N=1 might OOM. Mitigation: Tasks 76 and 127 measure working-set size empirically; if any chain exceeds available memory at N=1, that's a calculator-level bug (likely a missing predicate pushdown that's reading too much default-DB data into memory) which we fix at the calculator level rather than papering over with disk spill.

**Hidden RunSpec edge cases.** The MOVES RunSpec format has accumulated features over decades; some are rarely used and may have subtle behavioral implications. Mitigation: keep the canonical-MOVES Docker image around forever; any divergent RunSpec a user reports gets characterized against canonical and added to the fixture suite.

**Scope creep from EPA update tracking.** Once we're tracking upstream MOVES, every annual release is a project. Mitigation: the project explicitly bounds its support — we track major releases on a best-effort basis, we don't commit to feature parity with every patch release.

## Out of scope (explicitly)

- The MOVES GUI (Swing application, 37k Java).
- Distributed execution across multiple machines (`amazon/`, multi-host worker protocol).
- Uncertainty/Monte Carlo simulation (`ExecutionRunSpec.estimateUncertainty()`).
- The 508-accessibility test suite.
- Regulatory validation: this port is not for SIP submissions, transportation conformity determinations, or any other regulatory purpose.
- Disk-spill machinery for memory pressure (see Concurrency and memory model section above for the rationale).
- Fortran FFI in the runtime path (incompatible with the WASM target requirement).

If any of these become important later, they're additive scope on top of the Phase 7 baseline.

# Onroad Calculator and Generator Coverage

The moves-rust port implements every MOVES onroad emission calculator and
generator from the Java MOVES implementation, reimplemented in pure Rust with
no SQL dependency at runtime.

---

## Generators

All 15 generators from `CalculatorInfo.txt` are implemented, plus one
additional helper generator required by the framework:

| Java name | Rust module |
|-----------|-------------|
| `AverageSpeedOperatingModeDistributionGenerator` | `generators/avg_speed_op_mode_distribution.rs` |
| `BaseRateGenerator` | `generators/baserategenerator/` |
| `EvaporativeEmissionsOperatingModeDistributionGenerator` | `generators/evap_op_mode_distribution.rs` |
| `FuelEffectsGenerator` | `generators/fueleffectsgenerator/` |
| `LinkOperatingModeDistributionGenerator` | `generators/link_op_mode_distribution.rs` |
| `MesoscaleLookupOperatingModeDistributionGenerator` | `generators/mesoscale_lookup/` |
| `MesoscaleLookupTotalActivityGenerator` | `generators/mesoscale_lookup/` |
| `MeteorologyGenerator` | `generators/meteorology.rs` |
| `OperatingModeDistributionGenerator` | `generators/operating_mode_distribution/` |
| `RatesOperatingModeDistributionGenerator` | `generators/rates_op_mode_distribution.rs` |
| `SourceBinDistributionGenerator` | `generators/source_bin_distribution_generator.rs` |
| `StartOperatingModeDistributionGenerator` | `generators/start_operating_mode_distribution.rs` |
| `TankFuelGenerator` | `generators/tank_fuel_generator.rs` |
| `TankTemperatureGenerator` | `generators/tank_temperature_generator.rs` |
| `TotalActivityGenerator` | `generators/totalactivitygenerator/` |
| *(not in DAG)* `SourceTypePhysics` | `generators/sourcetypephysics.rs` |

`SourceTypePhysics` is an additional helper generator not recorded in
`CalculatorInfo.txt` but required by `LinkOperatingModeDistributionGenerator`.

---

## Calculators

All 38 `kind=Calculator` modules from `CalculatorInfo.txt` are implemented in
Rust:

| Java name | Rust struct |
|-----------|-------------|
| `ActivityCalculator` | `ActivityCalculator` |
| `AirToxicsCalculator` | `AirToxicsCalculator` |
| `AirToxicsDistanceCalculator` | `AirToxicsDistanceCalculator` |
| `BaseRateCalculator` | `BaseRateCalculator` |
| `BasicBrakeWearPMEmissionCalculator` | `BasicBrakeWearPmEmissionCalculator` |
| `BasicRunningPMEmissionCalculator` | `BasicRunningPmEmissionCalculator` |
| `BasicStartPMEmissionCalculator` | `BasicStartPmEmissionCalculator` |
| `BasicTireWearPMEmissionCalculator` | `BasicTireWearPmEmissionCalculator` |
| `CH4N2ORunningStartCalculator` | `Ch4N2oRunningStartCalculator` |
| `CH4N2OWTPCalculator` | `Ch4N2oWtpCalculator` |
| `CO2AERunningStartExtendedIdleCalculator` | `CO2AERunningStartExtendedIdleCalculator` |
| `CO2AtmosphericWTPCalculator` | `Co2AtmosphericWtpCalculator` |
| `CO2EqivalentWTPCalculator` | `Co2EquivalentWtpCalculator` |
| `CrankcaseEmissionCalculator` | *(Java base class — no direct Rust struct; split into NonPM + PM below)* |
| `CrankcaseEmissionCalculatorNonPM` | `CrankcaseEmissionCalculatorNonPM` |
| `CriteriaRunningCalculator` | `CriteriaRunningCalculator` |
| `CriteriaStartCalculator` | `CriteriaStartCalculator` |
| `DistanceCalculator` | `DistanceCalculator` |
| `DummyCalculator` | `DummyCalculator` (no-op) |
| `EvaporativePermeationCalculator` | `EvaporativePermeationCalculator` |
| `GenericCalculatorBase` | *(Java base class → Rust `Calculator` trait)* |
| `HCSpeciationCalculator` | `HcSpeciationCalculator` |
| `LiquidLeakingCalculator` | `LiquidLeakingCalculator` |
| `NH3RunningCalculator` | `Nh3RunningCalculator` |
| `NH3StartCalculator` | `Nh3StartCalculator` |
| `NO2Calculator` | `NO2Calculator` |
| `NOCalculator` | `NOCalculator` |
| `NRAirToxicsCalculator` | `NrAirToxicsCalculator` |
| `NRHCSpeciationCalculator` | `NrHcSpeciationCalculator` |
| `NonroadEmissionCalculator` | *(NONROAD path — `moves-nonroad` crate)* |
| `PM10BrakeTireCalculator` | `PM10BrakeTireCalculator` |
| `PM10EmissionCalculator` | `PM10EmissionCalculator` |
| `PMTotalExhaustCalculator` | `PmTotalExhaustCalculator` |
| `RefuelingLossCalculator` | `RefuelingLossCalculator` |
| `SO2Calculator` | `SO2Calculator` |
| `SulfatePMCalculator` | `SulfatePMCalculator` |
| `TOGSpeciationCalculator` | `TogSpeciationCalculator` |
| `TankVaporVentingCalculator` | `TankVaporVentingCalculator` |

Two additional Rust calculators exist in the Java MOVES source but were not
captured in the `CalculatorInfo.txt` snapshot: `CrankcaseEmissionCalculatorPM`
and `MultidayTankVaporVentingCalculator`. Their SQL files
(`CrankcaseEmissionCalculator.sql`, `MultidayTankVaporVentingCalculator.sql`)
are present in the upstream repo and their Rust ports are complete.

**Notes on exceptions:**

- `NonroadEmissionCalculator` — produces NONROAD exhaust emissions (Running
 Exhaust, Crankcase, Refueling for off-road equipment). The Rust port lives in
 `crates/moves-nonroad/`, which is a separate simulation path from the onroad
 calculator framework.
- `GenericCalculatorBase` / `CrankcaseEmissionCalculator` — Java abstract base
 classes that carry no computation; the Rust `Calculator` trait and the two
 concrete `CrankcaseEmission*` structs replace them.
- `DummyCalculator` — Java no-op placeholder with `registrations_count: 0` and
 no dependents. Ported as `DummyCalculator` with empty subscriptions and
 registrations; produces no output.

---

## Control strategies and other modules

The `CalculatorInfo.txt` DAG also contains control strategies and test
infrastructure that are not emission calculators or generators:

| Java name | Kind |
|-----------|------|
| `AVFTControlStrategy` | Control strategy (`moves-avft` crate) |
| `FuelControlStrategy` | Control strategy |
| `MasterLoopTest` | Test infrastructure — intentionally excluded |
| `OnRoadRetrofitStrategy` | Control strategy |
| `ProjectTAG` | Generator variant |
| `RateOfProgressStrategy` | Control strategy |
| `SourceMaintenanceControlStrategy` | Control strategy |
| `SourceManufacturingControlStrategy` | Control strategy |
| `SourceUsageControlStrategy` | Control strategy |
| `WellToPumpProcessor` | Processor |

Control strategies (AVFT, Fuel, OnRoadRetrofit, RateOfProgress, Source*)
adjust default-DB inputs rather than performing emission calculations.
`AVFTControlStrategy` is partially covered by the `moves-avft` crate.

---

## Characterization suite

The integration-validation gate (`cargo test -p moves-calculators --test
calculator_integration`) runs in CI:

- **Catalogue tests** — 38 calculators instantiate, names are unique.
- **Coverage matrix** — all 26 onroad fixtures exercise at least one calculator;
 37 calculators are covered by at least one fixture (`DummyCalculator` has no
 registrations and is in `KNOWN_UNCOVERED`).
- **Tolerance budget** — `characterization/calculator-validation/tolerance.toml`
 parses with `default_float_tolerance = 1e-9`; no divergences declared yet.
- **Canonical-capture diff** — dormant until the compute-node run populates
 `characterization/snapshots/`. The gate picks up snapshots automatically when
 present. See `characterization/calculator-validation/README.md`.

Generator gate (`cargo test -p moves-calculators --test generator_integration`):
16 generators, 23 fixture coverage matrix, tolerance budget parses.

---

## Performance

Baseline measurement: **2.8–4.7 ms per fixture, 11.6 MiB peak RSS** (pure
framework overhead). Full results in `docs/performance-baseline.md`.

Memory model validation: peak RSS stays flat at ~10.6 MiB regardless of
`--max-parallel-chunks` (1 to NCPU), confirming calculator chains share no
unexpected state. Results in `docs/concurrency-tuning.md`.

The 5–20× improvement over canonical MOVES is expected once the
default-database data-plane wiring removes MariaDB I/O; the baseline is
captured and the assertion infrastructure is in place.

---

## SQL handling

The Rust runtime (`moves-cli`, `moves-framework`, `moves-calculators`) has no
SQL dependency at runtime:

- No `*.sql` file is read by any production code path.
- `crates/moves-sql-macros/` is a developer tool (macro expander + section
 processor) that is not linked into the production binary. The Rust calculators
 do not run macro-templated SQL.
- The only SQL in the repository is `crates/moves-sql-macros/tests/fixtures/sample.sql`,
 a test fixture for the expander tool.

The original MOVES SQL files (GPL-licensed) are not committed to this MIT
repository but are documented in `reference/README.md` with a 34-entry mapping
from SQL file to Rust module, instructions for obtaining the SQL files via
`scripts/resolve_moves_src.sh`, and instructions for expanding SQL macros with
`moves-sql-expand` to see the canonical reference for each calculator's
computation.

---

## Known limitations

Full numeric validation against the canonical MOVES output requires a
compute-node run that populates `characterization/snapshots/`. That run
requires the Apptainer SIF (`characterization/apptainer/`) and the canonical
MOVES MariaDB default database. It can proceed independently of other code
work.

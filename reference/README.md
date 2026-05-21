# reference/ — Original MOVES SQL as documentation

This directory holds the **original MOVES SQL files** from EPA's
`EPA_MOVES_Model` repository (MOVES5.0.1, commit `25dc6c83`) as
documentation only. They are the canonical reference for what each Rust
calculator computes; they are **not on the runtime path**.

The SQL files are licensed under the GNU GPL and therefore not committed
to this MIT-licensed repository. Obtain them from the upstream MOVES
source checkout, which `scripts/resolve_moves_src.sh` locates or
downloads automatically:

```sh
MOVES_SRC=$(scripts/resolve_moves_src.sh)
ls "$MOVES_SRC/database/"   # the 83 calculator and schema SQL files
```

## Calculator SQL files (Phase 3 reference)

The Rust calculators in `crates/moves-calculators/src/calculators/` and
`crates/moves-calculators/src/generators/` port the following SQL files,
in coverage-map order (hottest first):

| SQL file | Rust module | Phase 3 task |
|----------|-------------|--------------|
| `BaseRateCalculator.sql` | `calculators/baseratecalculator/` | Task 45 |
| `CriteriaRunningCalculator.sql` | `calculators/criteria_running_calculator.rs` | Task 46 |
| `HCSpeciationCalculator.sql` | `calculators/hcspeciation.rs` | Task 48 |
| `ActivityCalculator.sql` | `calculators/activitycalculator/` | Task 71 |
| `CriteriaStartCalculator.sql` | `calculators/criteria_start_calculator.rs` | Task 47 |
| `NRHCSpeciationCalculator.sql` | `calculators/nrhcspeciation.rs` | Task 49 |
| `AirToxicsCalculator.sql` | `calculators/airtoxics.rs` | Task 50 |
| `AirToxicsDistanceCalculator.sql` | `calculators/airtoxicsdistance.rs` | Task 51 |
| `NRAirToxicsCalculator.sql` | `calculators/nrairtoxics.rs` | Task 52 |
| `PMTotalExhaustCalculator.sql` | `calculators/pmexhaust/` | Task 53 |
| `BasicPM25Calculator.sql` | `calculators/pmexhaust/`, `calculators/basicbraketirepm.rs` | Task 53 / 56 |
| `BasicStartPM25Calculator.sql` | `calculators/basicstartpm.rs` | Task 54 |
| `PM10EmissionCalculator.sql` | `calculators/pm10.rs` | Task 55 |
| `PM10BrakeTireCalculator.sql` | `calculators/pm10.rs` | Task 55 |
| `SulfatePMCalculator.sql` | `calculators/sulfate_pm_calculator.rs` | Task 57 |
| `EvaporativePermeationCalculator.sql` | `calculators/evaporative_permeation_calculator.rs` | Task 58 |
| `TankVaporVentingCalculator.sql` | `calculators/tank_vapor_venting_calculator.rs` | Task 59 |
| `MultidayTankVaporVentingCalculator.sql` | `calculators/multiday_tank_vapor_venting_calculator.rs` | Task 60 |
| `LiquidLeakingCalculator.sql` | `calculators/liquid_leaking_calculator.rs` | Task 61 |
| `RefuelingLossCalculator.sql` | `calculators/refueling_loss_calculator.rs` | Task 62 |
| `CrankcaseEmissionCalculator.sql` | `calculators/crankcase_emission.rs` | Task 63 |
| `CO2AERunningStartExtendedIdleCalculator.sql` | `calculators/co2ae_running_start_extended_idle.rs` | Task 64 |
| `CH4N2ORunningStartCalculator.sql` | `calculators/ch4n2o_running_start.rs` | Task 65 |
| `NH3RunningCalculator.sql` | `calculators/nh3/` | Task 66 |
| `NH3StartCalculator.sql` | `calculators/nh3/` | Task 66 |
| `SO2Calculator.sql` | `calculators/so2_calculator.rs` | Task 67 |
| `NOCalculator.sql` | `calculators/nitrogen_oxide.rs` | Task 68 |
| `NO2Calculator.sql` | `calculators/nitrogen_oxide.rs` | Task 68 |
| `WellToPumpCalculator.sql` | `calculators/welltopump/` | Task 69 |
| `CH4N2OWTPCalculator.sql` | `calculators/welltopump/` | Task 69 |
| `CO2AtmosphericWTPCalculator.sql` | `calculators/welltopump/` | Task 69 |
| `CO2EqivalentWTPCalculator.sql` | `calculators/welltopump/` | Task 69 |
| `TOGSpeciationCalculator.sql` | `calculators/togspeciation.rs` | Task 70 |
| `DistanceCalculator.sql` | `calculators/distance_calculator.rs` | Task 72 |

## Expanding SQL macros

To see what a SQL file looks like after MOVES macro-expansion (the form
the Rust calculators implement), use `moves-sql-expand`:

```sh
cargo run --release --bin moves-sql-expand -- \
    --input "$MOVES_SRC/database/BaseRateCalculator.sql" \
    --section Inventory
```

The expander is in `crates/moves-sql-macros` and is **not on the runtime
path** — the Rust calculators use Polars expressions, not SQL.

## Runtime-path status

The Rust runtime (`moves-cli`, `moves-framework`, `moves-calculators`) has
**no SQL dependency**. The only SQL in this repository is:

- `crates/moves-sql-macros/` — the macro-expander tool (developer aid only)
- `crates/moves-sql-macros/tests/fixtures/sample.sql` — test fixture

Neither path is linked into the production binary.

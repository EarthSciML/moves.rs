//! `moves-calculators` — onroad emission calculators and generators ported
//! from Java.
//!
//! Hosts the calculator and generator implementations under
//! `gov/epa/otaq/moves/master/implementation/ghg/` and related packages.
//! Each module declares the `(pollutant, process)` pairs it produces (for
//! calculators) or the scratch tables it writes (for generators), plus the
//! granularity at which it subscribes to the master loop; `moves-framework`
//! drives them according to the chain reconstructed in Phase 1
//! (Task 10, `moves-calculator-info`).
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Phase 3 — generator ports (Tasks 29–44) run before the calculator
//!   ports (Tasks 30–88) in the master loop.
//!
//! # Phase 3 status
//!
//! Implementation tasks land per-module across Phase 3:
//!
//! * Task 38 — [`TankTemperatureGenerator`], the soak-mode tank-temperature
//!   and activity-fraction generator. The ported computation is the pure
//!   [`generate_tank_temperatures`] function; the
//!   [`Generator`](moves_framework::Generator) adapter's `execute` stays an
//!   empty stand-in until the Task 50 `DataFrameStore` data plane lands.

pub mod tank_temperature_generator;

pub use tank_temperature_generator::{
    calculate_cold_soak_tank_temperature, generate_tank_temperatures, AverageTankTemperatureRow,
    ColdSoakInitialHourFractionRow, ColdSoakTankTemperatureRow, HourDayRow, SampleVehicleDayRow,
    SampleVehicleTripRow, SoakActivityFractionRow, SourceTypeModelYearGroupRow,
    TankTemperatureGenerator, TankTemperatureInputs, TankTemperatureOutput, TankTemperatureRiseRow,
    ZoneMonthHourRow,
};

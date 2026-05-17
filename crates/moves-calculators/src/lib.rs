//! `moves-calculators` — onroad emission calculators and generators ported
//! from Java and Go.
//!
//! Hosts the ~70 calculator implementations under
//! `gov/epa/otaq/moves/master/implementation/ghg/` and related packages,
//! plus the generators that run ahead of them in the master loop. Each
//! module declares the `(pollutant, process)` pairs it produces and the
//! granularity at which it subscribes to the master loop; `moves-framework`
//! drives them according to the chain reconstructed in Phase 1
//! (Task 10, `moves-calculator-info`).
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Phase 3 — Tasks 29–43 cover the generators, Tasks 45–88 the calculators.
//!
//! # Phase 3 status
//!
//! The crate is filled in module by module by the Phase 3 implementation
//! tasks. The [`generators`] module hosts the generator ports; calculator
//! ports land alongside them as Phase 3 progresses.

pub mod error;
pub mod generators;
pub mod tank_fuel_generator;
pub mod tank_temperature_generator;

pub use error::{Error, Result};
pub use tank_fuel_generator::{
    calculate_average_tank_gasoline, AverageTankGasolineRow, FuelFormulationRow, FuelSubtypeRow,
    FuelSupplyRow, FuelTypeRow, MonthOfAnyYearRow, RegionCountyRow, TankFuelGenerator,
    TankFuelInputs, YearRow, ZoneMonthHourRow, ZoneRow,
};
// `tank_temperature_generator` defines its own `ZoneMonthHourRow` — a distinct
// type, it carries `hour_id` — so it is not re-exported here: the crate-root
// `ZoneMonthHourRow` is `tank_fuel_generator`'s. Reach the other via its module.
pub use tank_temperature_generator::{
    calculate_cold_soak_tank_temperature, generate_tank_temperatures, AverageTankTemperatureRow,
    ColdSoakInitialHourFractionRow, ColdSoakTankTemperatureRow, HourDayRow, SampleVehicleDayRow,
    SampleVehicleTripRow, SoakActivityFractionRow, SourceTypeModelYearGroupRow,
    TankTemperatureGenerator, TankTemperatureInputs, TankTemperatureOutput, TankTemperatureRiseRow,
};

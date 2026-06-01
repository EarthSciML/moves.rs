//! Well-To-Pump (WTP) emission calculators —.
//!
//! Ports the four calculators the groups into, which
//! together model **well-to-pump (upstream) emissions** — the energy and
//! greenhouse gases spent extracting, refining and distributing a fuel before
//! it ever reaches a vehicle's tank, as distinct from the pump-to-wheel
//! emissions the vehicle itself produces:
//!
//! * [`total_energy::WellToPumpProcessor`] — `WellToPumpProcessor`, the
//! well-to-pump Total Energy Consumption (pollutant 91).
//! * [`ch4n2o::Ch4N2oWtpCalculator`] — `CH4N2OWTPCalculator`, well-to-pump
//! methane (5) and nitrous oxide (6).
//! * [`co2_atmospheric::Co2AtmosphericWtpCalculator`]//! `CO2AtmosphericWTPCalculator`, well-to-pump atmospheric CO2 (90).
//! * [`co2_equivalent::Co2EquivalentWtpCalculator`]//! `CO2EqivalentWTPCalculator`, well-to-pump CO2 equivalent (98).
//!
//! # Two steps
//!
//! The cluster runs in two logical steps. The first three calculators each
//! scale a vehicle pump-to-wheel quantity (`MOVESWorkerOutput` Total Energy
//! Consumption) by a fuel-specific GREET well-to-pump factor to produce a
//! well-to-pump pollutant on process 99. The fourth,
//! [`co2_equivalent::Co2EquivalentWtpCalculator`], is a second step: it reads
//! those process-99 atmospheric CO2, methane and nitrous oxide rows back and
//! sums them, each weighted by its global warming potential, into well-to-pump
//! CO2 equivalent.
//!
//! [`total_energy`] and [`ch4n2o`] share the GREET year-interpolation and
//! market-share weighting that builds `WTPFactorByFuelType`; that, the
//! `MOVESWorkerOutput` row shape and the default-DB input tables live in
//! [`common`]. [`co2_atmospheric`] uses the same input tables but its own
//! non-interpolating factor build; [`co2_equivalent`] needs neither.
//!
//! # Superseded by `BaseRateCalculator`
//!
//! None of the four calculators is wired into the pinned MOVES runtime. The
//! Well-To-Pump process (id 99) has **no `Registration` directive at all** in
//! `CalculatorInfo.txt`, and `characterization/calculator-chains/calculator-dag.json`
//! records every WTP module with `registrations_count: 0`, `subscriptions: []`
//! and `depends_on: []`. The modern base-rate engine (`BaseRateCalculator`,
//! ) superseded the older per-pollutant scripted-SQL
//! calculators. The still lists the four classes as, so
//! this module ports their algorithms faithfully for reference and
//! cross-validation, with each calculator's
//! [`Calculator::registrations`](moves_framework::Calculator::registrations),
//! `subscriptions` and `upstream` returning empty slices. See each calculator
//! module's supersession note.

pub mod ch4n2o;
pub mod co2_atmospheric;
pub mod co2_equivalent;
pub mod common;
pub mod total_energy;

pub use ch4n2o::Ch4N2oWtpCalculator;
pub use co2_atmospheric::{build_co2_factor_by_fuel_type, Co2AtmosphericWtpCalculator};
pub use co2_equivalent::{Co2EquivalentWtpCalculator, Co2EquivalentWtpInputs, PollutantGwpRow};
pub use common::{
    build_wtp_factor_by_fuel_type, month_group_index, FuelFormulationRow, FuelSubTypeRow,
    FuelSupplyRow, GreetWellToPumpRow, MonthGroupRow, WorkerOutputRow, WtpFactorCell,
    WtpFactorTable, WtpInputs, YearRow,
};
pub use total_energy::WellToPumpProcessor;

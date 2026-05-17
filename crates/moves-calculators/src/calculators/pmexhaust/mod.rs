//! Exhaust particulate-matter calculators — Phase 3 Task 53.
//!
//! Ports the two PM-exhaust calculators the migration plan groups into
//! Task 53:
//!
//! * [`total::PmTotalExhaustCalculator`] — `PMTotalExhaustCalculator`, the
//!   chained calculator that forms the PM10 and PM2.5 *totals* (pollutants
//!   100 and 110) by re-labelling their organic-carbon / elemental-carbon /
//!   sulfate component rows.
//! * [`running::BasicRunningPmEmissionCalculator`] —
//!   `BasicRunningPMEmissionCalculator`, the direct subscriber that computes
//!   the *running-exhaust* PM2.5 components (elemental carbon 112 and
//!   composite non-EC 118) by the activity × emission-rate methodology.
//!
//! The two are independent calculators with no shared code; they live in one
//! module because Task 53 ports them together. `BasicStartPMEmissionCalculator`
//! — the start-exhaust counterpart — is a separate task (migration-plan
//! Task 54) and is not ported here.
//!
//! Each calculator's module documents its source files, the SQL pipeline it
//! ports, its chain metadata, and the Task 50 data-plane status.

pub mod running;
pub mod total;

pub use running::{
    AgeCategoryRow, BasicRunningPmEmissionCalculator, BasicRunningPmInputs, CountyRow,
    EmissionRateByAgeRow, FuelFormulationRow, FuelSubTypeRow, FuelSupplyRow, GeneralFuelRatioRow,
    HourDayRow, LinkRow, MonthOfAnyYearRow, MovesWorkerOutputRow, OpModeDistributionRow,
    PollutantProcessAssocRow, PollutantProcessModelYearRow, RunContext, RunSpecSourceTypeRow,
    ShoRow, SourceBinDistributionRow, SourceBinRow, SourceTypeModelYearRow,
    TemperatureAdjustmentRow, YearRow, ZoneMonthHourRow,
};
pub use total::{PmTotalExhaustCalculator, PmWorkerRow, TotalSelection};

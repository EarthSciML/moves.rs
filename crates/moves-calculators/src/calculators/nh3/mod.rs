//! Ammonia (NH3) exhaust calculators — Phase 3 Task 66.
//!
//! Ports the two ammonia calculators the migration plan groups into Task 66:
//!
//! * [`running::Nh3RunningCalculator`] — `NH3RunningCalculator`, the
//!   running-exhaust ammonia calculator (process 1).
//! * [`start::Nh3StartCalculator`] — `NH3StartCalculator`, the start-exhaust
//!   ammonia calculator (process 2).
//!
//! Both are thin `GenericCalculatorBase` subclasses driving near-identical
//! scripts (`database/NH3RunningCalculator.sql`,
//! `database/NH3StartCalculator.sql`). They share their I/M-coverage merge,
//! source-bin weighting and I/M blend, which live in [`common`]; the
//! running- and start-specific operating-mode weighting and activity multiply
//! live in [`running`] and [`start`].
//!
//! # Superseded by `BaseRateCalculator`
//!
//! Neither calculator is wired into the pinned MOVES runtime. `CalculatorInfo.txt`
//! registers `Ammonia (NH3)` on Running Exhaust and Start Exhaust to
//! `BaseRateCalculator` (migration-plan Task 45), the modern base-rate
//! calculator that superseded the older per-pollutant scripted-SQL
//! calculators; `characterization/calculator-chains/calculator-dag.json`
//! records both NH3 modules with `registrations_count: 0`. The migration plan
//! still lists the classes as Task 66, so this module ports their algorithms
//! faithfully for reference and cross-validation, with each calculator's
//! [`Calculator::registrations`](moves_framework::Calculator::registrations)
//! returning an empty slice. See each calculator module's supersession note.

pub mod common;
pub mod running;
pub mod start;

pub use common::{
    finalize_with_im, merge_im_coverage, weight_by_source_bin, AgeCategoryRow,
    EmissionRateByAgeRow, EmissionRow, HourDayRow, ImCoverageMergedRow, ImCoverageRow, ImFactorRow,
    OpModeDistributionRow, PollutantProcessAssocRow, PollutantProcessMappedModelYearRow,
    SourceBinDistributionRow, SourceBinEmissionRate, SourceBinRow, SourceTypeModelYearRow,
    NH3_POLLUTANT_ID,
};
pub use running::{LinkRow, Nh3RunningCalculator, RunningContext, RunningInputs, ShoRow};
pub use start::{Nh3StartCalculator, StartContext, StartInputs, StartsRow};

//! `moves-onroad-retrofit` — OnRoadRetrofit internal control strategy
//! (Phase 6 Task 122).
//!
//! Ports `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies
//! .onroadretrofit.OnRoadRetrofit` from the Java MOVES source
//! (`internalcontrolstrategies/onroadretrofit/`, ~700 lines).
//!
//! # What this does
//!
//! The OnRoadRetrofit control strategy models retrofit emission-control
//! programs for on-road vehicles. A retrofit program specifies:
//!
//! - Which source types and model-year range the program targets
//! - What fraction of the matching fleet has been retrofitted by a given year
//! - How effective the retrofit device is at reducing emissions for a specific
//!   pollutant/process pair
//!
//! The combined emission adjustment factor for a given
//! `(sourceType, modelYear, pollutant, process)` combination is:
//!
//! ```text
//! factor = ∏ over active programs p of (1 - p.fraction * p.effectiveness)
//! ```
//!
//! A factor of `1.0` means no reduction; `0.6` means 40% fewer emissions.
//!
//! # Usage
//!
//! ```no_run
//! use moves_onroad_retrofit::{OnRoadRetrofitStrategy, RetrofitRecord, RetrofitTable};
//!
//! let programs: RetrofitTable = vec![
//!     RetrofitRecord::new(
//!         11,    // sourceTypeID: passenger cars
//!         2005,  // startModelYear
//!         2015,  // endModelYear
//!         2020,  // retrofitYearID
//!         98,    // pollutantID: CO2 equivalent
//!         1,     // processID: running exhaust
//!         0.25,  // 25% of fleet retrofitted by 2020
//!         0.80,  // 80% emission reduction per retrofitted vehicle
//!     ),
//! ]
//! .into_iter()
//! .collect();
//!
//! let strategy = OnRoadRetrofitStrategy::new(programs);
//! // Register with MOVESEngine via ControlStrategyRegistry
//! ```
//!
//! # Data-plane status
//!
//! The write of computed adjustment factors into `emissionRateAdjustment` is
//! deferred to Task 50 (`DataFrameStore`). See
//! [`control_strategy::OnRoadRetrofitStrategy::pre_run`] for the TODO
//! comment.

pub mod control_strategy;
pub mod error;
pub mod model;

pub use control_strategy::OnRoadRetrofitStrategy;
pub use error::{Error, Result};
pub use model::{
    ModelYearId, PollutantId, ProcessId, RetrofitKey, RetrofitRecord, RetrofitTable,
    RetrofitYearId, SourceTypeId,
};

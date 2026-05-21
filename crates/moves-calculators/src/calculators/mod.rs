//! MOVES emission calculators — the per-`(pollutant, process)` modules
//! that run inside the master loop, consuming generator output and the
//! filtered execution database to produce the emission and activity
//! records the output processor writes.
//!
//! Ports `moves-rust-migration-plan.md` Phase 3 Tasks 45–88. Each
//! calculator lives in its own submodule and implements
//! [`moves_framework::Calculator`]; a port registers its module with a
//! single `pub mod` line here, keeping the crate root (`lib.rs`) a stable
//! area list.

pub mod activitycalculator;
pub mod airtoxics;
pub mod airtoxicsdistance;
pub mod baseratecalculator;
pub mod dummy;
pub mod basicbraketirepm;
pub mod basicstartpm;
pub mod ch4n2o_running_start;
pub mod co2ae_running_start_extended_idle;
pub mod crankcase_emission;
pub mod criteria_running_calculator;
pub mod criteria_start_calculator;
pub mod distance_calculator;
pub mod evaporative_permeation_calculator;
pub mod hcspeciation;
pub mod liquid_leaking_calculator;
pub mod multiday_tank_vapor_venting_calculator;
pub mod nh3;
pub mod nitrogen_oxide;
pub mod nrairtoxics;
pub mod nrhcspeciation;
pub mod pm10;
pub mod pmexhaust;
pub mod refueling_loss_calculator;
pub mod so2_calculator;
pub mod sulfate_pm_calculator;
pub mod tank_vapor_venting_calculator;
pub mod togspeciation;
pub mod welltopump;

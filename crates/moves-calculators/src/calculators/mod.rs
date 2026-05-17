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
pub mod baseratecalculator;
pub mod ch4n2o_running_start;
pub mod distance_calculator;
pub mod evaporative_permeation_calculator;
pub mod nrairtoxics;
pub mod nrhcspeciation;
pub mod so2_calculator;

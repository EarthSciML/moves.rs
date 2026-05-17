//! Mesoscale-Lookup generators — Phase 3 Task 35.
//!
//! Ports the two `gov/epa/otaq/moves/master/implementation/ghg/` classes
//! that build the activity and operating-mode tables for runs using the
//! Mesoscale-Lookup output domain (where each link's average-speed bin is
//! looked up rather than derived):
//!
//! * [`op_mode_distribution`] — `MesoscaleLookupOperatingModeDistributionGenerator`,
//!   the operating-mode-fraction table;
//! * [`total_activity`] — `MesoscaleLookupTotalActivityGenerator`, the
//!   `SHO` / `SourceHours` activity-basis tables.
//!
//! Each submodule ports one generator: the numerically faithful compute
//! core as tested free functions, plus the [`moves_framework::Generator`]
//! implementation carrying real master-loop metadata. See the submodule
//! documentation for the per-generator algorithm and the Task 50
//! data-plane boundary.

pub mod op_mode_distribution;
pub mod total_activity;

//! MOVES generators — the modules that run ahead of the emission
//! calculators in the master loop, producing the activity, operating-mode,
//! and rate tables the calculators consume.
//!
//! Ports `moves-rust-migration-plan.md` Phase 3 Tasks 29–43. Each generator
//! lives in its own submodule and implements [`moves_framework::Generator`].

pub mod avg_speed_op_mode_distribution;
pub mod baserategenerator;
pub mod meteorology;
pub mod rates_op_mode_distribution;
pub mod source_bin_distribution_generator;
pub mod tank_fuel_generator;
pub mod tank_temperature_generator;

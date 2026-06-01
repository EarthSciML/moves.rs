//! MOVES generators — the modules that run ahead of the emission
//! calculators in the master loop, producing the activity, operating-mode,
//! and rate tables the calculators consume.
//!
//! Ports `moves-rust-.md`. Each generator
//! lives in its own submodule and implements [`moves_framework::Generator`].

pub mod avg_speed_op_mode_distribution;
pub mod baserategenerator;
pub mod evap_op_mode_distribution;
pub mod fueleffectsgenerator;
pub mod link_op_mode_distribution;
pub mod mesoscale_lookup;
pub mod meteorology;
pub mod new_tvv_year_generator;
pub mod operating_mode_distribution;
pub mod project_tag;
pub mod rates_op_mode_distribution;
pub mod source_bin_distribution_generator;
pub mod sourcetypephysics;
pub mod start_operating_mode_distribution;
pub mod tank_fuel_generator;
pub mod tank_temperature_generator;
pub mod totalactivitygenerator;

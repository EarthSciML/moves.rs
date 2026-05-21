//! `moves-nonroad-retrofit` — NonRoadRetrofit internal control strategy
//! (Phase 6 Task 123).
//!
//! Wires NONROAD's existing retrofit support (ported from `clcrtrft.f` in
//! Task 108) into the unified control-strategy framework so that a single
//! RunSpec retrofit declaration can drive both the onroad and nonroad
//! calculators where applicable.
//!
//! # What this does
//!
//! The NONROAD model has its own retrofit calculation (ported in Task 108)
//! that applies emission-reduction factors based on:
//!
//! - SCC code (or the `ALL` wildcard)
//! - Tech type (or the `ALL` wildcard)
//! - HP range (non-inclusive lower, inclusive upper)
//! - Model-year range
//! - Retrofit year range
//! - Pollutant (HC / CO / NOX / PM)
//! - Annual fraction retrofitted OR count retrofitted
//! - Retrofit effectiveness (0.0–1.0)
//!
//! This crate wraps those records as an [`InternalControlStrategy`] so the
//! framework can discover and drive the strategy through its lifecycle hooks.
//! The actual per-SCC reduction computation stays in
//! [`moves_nonroad::emissions::retrofit`]; this crate provides the framework
//! adapter.
//!
//! # Usage
//!
//! ```no_run
//! use moves_nonroad_retrofit::NonRoadRetrofitStrategy;
//! use moves_nonroad::population::retrofit::RetrofitRecord;
//!
//! let records = vec![/* load from .RTR file via moves_nonroad::input::retrofit */];
//! let strategy = NonRoadRetrofitStrategy::new(records);
//! // Register with ControlStrategyRegistry via registry.register(|| Box::new(...))
//! ```

pub mod control_strategy;

pub use control_strategy::NonRoadRetrofitStrategy;

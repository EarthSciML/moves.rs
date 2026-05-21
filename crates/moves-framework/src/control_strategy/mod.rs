//! Control-strategy framework — `InternalControlStrategy` trait, registry,
//! and subscription types.
//!
//! See [`traits`] and [`registry`] for the public API.

pub mod registry;
pub mod traits;

pub use registry::{ControlStrategyFactory, ControlStrategyRegistry};
pub use traits::{InternalControlStrategy, StrategySubscription};

//! Master loop: the granularity-ordered iteration engine that drives
//! calculators and generators.

pub mod master_loop;

pub use master_loop::{
    Granularity, MasterLoop, MasterLoopContext, MasterLoopable, MasterLoopableSubscription,
};

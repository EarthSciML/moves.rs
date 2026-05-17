//! MOVES generators — the modules that run ahead of the emission
//! calculators in the master loop, producing the activity, operating-mode,
//! and rate tables the calculators consume.
//!
//! Ports `moves-rust-migration-plan.md` Phase 3 Tasks 29–43. Each generator
//! lives in its own submodule and implements [`moves_framework::Generator`].

pub mod fueleffectsgenerator;

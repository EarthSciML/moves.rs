//! `moves-calculators` — onroad emission calculators and generators ported
//! from Java and Go.
//!
//! Hosts the ~70 calculator implementations under
//! `gov/epa/otaq/moves/master/implementation/ghg/` and related packages,
//! plus the generators that run ahead of them in the master loop. Each
//! module declares the `(pollutant, process)` pairs it produces and the
//! granularity at which it subscribes to the master loop; `moves-framework`
//! drives them according to the chain reconstructed in Phase 1
//! (Task 10, `moves-calculator-info`).
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Phase 3 — Tasks 29–43 cover the generators, Tasks 45–88 the calculators.
//!
//! # Phase 3 status
//!
//! The crate is filled in module by module by the Phase 3 implementation
//! tasks, grouped into two areas: the [`generators`] module hosts the
//! generator ports (Tasks 29–43) and the [`calculators`] module hosts the
//! calculator ports (Tasks 45–88). Each port adds its module under the
//! relevant area and registers it with a single `pub mod` line in that
//! area's `mod.rs`, never in this file — so the crate root stays a stable,
//! merge-conflict-free area list as Phase 3 grows.

pub mod calculators;
pub mod error;
pub mod generators;

pub use error::{Error, Result};

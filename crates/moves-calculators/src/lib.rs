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
//! tasks. The [`meteorology`] and [`generators`] modules host the generator
//! ports landed so far; calculator ports land alongside them as Phase 3
//! progresses.

pub mod error;
pub mod generators;
pub mod meteorology;

pub use error::{Error, Result};

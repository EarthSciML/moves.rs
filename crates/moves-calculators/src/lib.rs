//! `moves-calculators` — onroad emission calculators ported from Java.
//!
//! Hosts the ~70 calculator implementations under
//! `gov/epa/otaq/moves/master/implementation/ghg/` and related packages.
//! Each calculator declares the `(pollutant, process)` pairs it produces and
//! the granularity at which it subscribes to the master loop; `moves-framework`
//! drives them according to the chain reconstructed in Phase 1
//! (Task 10, `moves-calculator-info`).
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Phase 3 — Tasks 30–88 cover the individual calculator ports.
//!
//! # Phase 3 status
//!
//! Skeleton crate. Implementation tasks land per-calculator across Phase 3.

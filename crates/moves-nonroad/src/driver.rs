//! Top-level driver loop.
//!
//! Cluster 1 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.1). Owns the SCC × geography × year
//! iteration that ties parsing, calculation, and writing together.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `nonroad.f`  | 397 | Main entry point; orchestration |
//! | `dayloop.f`  | 126 | Day-of-year loop |
//! | `daymthf.f`  | 194 | Month → day fractioning |
//! | `dispit.f`   |  50 | Iteration dispatch |
//! | `mspinit.f`  |   — | State-pollutant iteration init |
//! | `spinit.f`   |   — | Pollutant iteration init |
//! | `scrptime.f` | 212 | Scrappage-time accounting |
//!
//! # Naming note
//!
//! The migration plan (`moves-rust-migration-plan.md`, Task 91)
//! refers to this cluster as `moves-nonroad::main`. The actual
//! module is named `driver` to avoid collision with the `fn main()`
//! function in `src/main.rs`. See `ARCHITECTURE.md` § 2.1 for the
//! discussion.
//!
//! # Status
//!
//! Phase 5 skeleton — no code yet. Implementation lands in Task 113.

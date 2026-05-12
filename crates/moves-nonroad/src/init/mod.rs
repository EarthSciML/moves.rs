//! NONROAD initialisation orchestration.
//!
//! Task 99. The Fortran source partitions the initialisation work
//! across three files:
//!
//! | File | Role |
//! |---|---|
//! | `intnon.f` | High-level sequencer that calls every `rd*.f` parser in order |
//! | `intadj.f` | Populates the sulfur and RFG-adjustment factor tables |
//! | `intams.f` | Initialises AMS-format output parameters from `/PERIOD/` |
//!
//! Plus the equipment SCC code table from `iniasc.f`, which lives in
//! [`crate::common::eqpcod`].
//!
//! # Layout
//!
//! - [`intadj`] — sulfur and RFG-adjustment table initialiser.
//! - [`intams`] — AMS output parameter initialiser.
//! - [`intnon`] — high-level orchestrator that sequences the
//!   options-file packet parsers.
//!
//! # Status
//!
//! Task 99 implemented as a scoped orchestrator: the
//! options-file-resident packets are parsed in the same order as the
//! Fortran source. Per-file loaders (allocation, indicator, growth,
//! activity, population, emission factors, BSFC, seasonality, tech,
//! evap tech, daily, retrofit) are reachable through their existing
//! modules in [`crate::input`]; the full driver wires them up in
//! Task 113.

pub mod intadj;
pub mod intams;
pub mod intnon;

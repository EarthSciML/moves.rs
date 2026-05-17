//! `moves-nonroad` — pure-Rust port of EPA's NONROAD2008a
//! nonroad-emissions model.
//!
//! This crate replaces the Fortran `nonroad.exe` binary
//! (118 `.f` files, ~29.4k lines, plus 11 `.inc` files defining 65
//! named COMMON blocks) with a library that exposes a
//! `run_simulation` entry point for direct in-process invocation by
//! the moves-rs orchestrator (Phase 2). No subprocess, no scratch
//! files, no MariaDB ingestion.
//!
//! See [`ARCHITECTURE.md`](https://github.com/EarthSciML/moves.rs/blob/main/crates/moves-nonroad/ARCHITECTURE.md)
//! (next to this crate's `Cargo.toml`) for the full source-to-module
//! map and the cross-cutting policies on array sizes, error handling,
//! I/O, and WASM compatibility.
//!
//! # Phase 5 status
//!
//! This crate is being filled in module by module by the Phase 5
//! implementation tasks (92–118), on top of the Task 91 skeleton
//! (module structure plus shared types — [`Error`], [`Result`],
//! [`common::NonroadContext`]).
//!
//! [`run_simulation`] — the single in-process entry point — is the
//! Task 117 integration step ([`simulation`]). It ports `nonroad.f`'s
//! two-level driver loop (the outer `getpop` SCC-group loop and the
//! inner record loop) on top of the Task 113 record-loop planner, and
//! defines the [`NonroadOptions`] / [`NonroadInputs`] /
//! [`NonroadOutputs`] integration types that replace the Java↔Fortran
//! bridge's `.opt`-file generation, input-file generation, and MariaDB
//! ingestion. The six geography routines are reached through the
//! [`simulation::GeographyExecutor`] seam; the production executor
//! that builds their callback contexts from loaded reference data is a
//! following increment (see the [`simulation`] module docs).

pub mod allocation;
pub mod common;
pub mod driver;
pub mod emissions;
pub mod error;
pub mod geography;
pub mod init;
pub mod input;
pub mod output;
pub mod population;
pub mod simulation;
pub mod util;

pub use error::{Error, Result};
pub use simulation::{run_simulation, NonroadInputs, NonroadOptions, NonroadOutputs};

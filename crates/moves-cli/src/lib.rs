//! `moves-cli` — command-line driver that ties RunSpec parsing, calculator
//! execution, and output writing together.
//!
//! The binary lives in `src/main.rs`; the library surface holds reusable
//! command logic so integration tests can exercise it without spawning a
//! subprocess.
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 11 — Workspace and project skeleton (this crate).
//! * Phase 2 onward — wire up RunSpec → execution → output as those tasks land.
//!
//! # Phase 2 status
//!
//! Skeleton crate. The CLI currently only prints a placeholder; real subcommands
//! land alongside the runtime pieces in Phase 2.

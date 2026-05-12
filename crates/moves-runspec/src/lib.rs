//! `moves-runspec` — MOVES RunSpec parser, serializer, and TOML/XML converters.
//!
//! Will port `gov/epa/otaq/moves/master/runspec/` (23 Java files, ~5k lines)
//! to Rust using `quick-xml` + `serde`. The crate also defines the canonical
//! TOML schema isomorphic to the XML RunSpec but human-friendlier (named-enum
//! values, comments allowed); bidirectional converters keep the two in sync.
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 12 — RunSpec XML parser
//! * Task 13 — TOML-based RunSpec format
//! * Task 14 — Pollutant/process/source-type/road-type enums (consumed here)
//!
//! # Phase 2 status
//!
//! Skeleton crate. Implementation lands in Tasks 12 and 13.

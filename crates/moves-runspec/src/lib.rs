//! `moves-runspec` — MOVES RunSpec parser and serializer.
//!
//! Ports `gov/epa/otaq/moves/master/runspec/` (23 Java files, ~5k lines) to
//! Rust using `quick-xml`. The crate also defines the canonical TOML schema
//! isomorphic to the XML RunSpec but human-friendlier (named-enum values,
//! comments allowed); bidirectional converters keep the two in sync.
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 12 — RunSpec XML parser (this crate)
//! * Task 13 — TOML-based RunSpec format (lands next)
//! * Task 14 — Pollutant/process/source-type/road-type enums (consumed here)
//!
//! # Round-trip contract
//!
//! Parsing then re-serializing then re-parsing yields the same in-memory
//! [`RunSpec`]. The serializer's output format is canonical, so
//! `serialize → parse → serialize` is byte-identical even when the original
//! XML used different whitespace or attribute order.
//!
//! ```no_run
//! use moves_runspec::{parse_runspec, serialize_runspec};
//!
//! let xml = std::fs::read("characterization/fixtures/sample-runspec.xml").unwrap();
//! let spec = parse_runspec(&xml).unwrap();
//! let again = serialize_runspec(&spec).unwrap();
//! let reparsed = parse_runspec(&again).unwrap();
//! assert_eq!(spec, reparsed);
//! ```

#![warn(missing_docs)]

pub mod error;
mod parse;
mod serialize;
pub mod types;

pub use crate::error::{Error, Result};
pub use crate::parse::parse_runspec;
pub use crate::serialize::serialize_runspec;
pub use crate::types::*;

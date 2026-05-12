//! `moves-runspec` — MOVES RunSpec parser, serializer, and TOML/XML converters.
//!
//! Implements the canonical [`RunSpec`] model plus two surface formats:
//!
//! * **TOML** — the recommended, hand-authored format (Task 13). Short
//!   table names, named-enum values, supports comments.
//! * **XML** — the legacy MOVES `.mrs` / `.xml` format kept for
//!   compatibility with the characterization fixtures (Task 12 will
//!   extend the XML coverage to the full 23-file Java source set).
//!
//! Conversion is always model-mediated, so XML↔TOML round-trips through
//! a single [`RunSpec`] value are isomorphic by construction. See
//! `docs/runspec-toml.md` for the format mapping.
//!
//! # Example
//!
//! ```no_run
//! use moves_runspec::{from_xml_str, to_toml_string};
//!
//! let xml = std::fs::read_to_string("sample-runspec.xml").unwrap();
//! let spec = from_xml_str(&xml).unwrap();
//! let toml = to_toml_string(&spec).unwrap();
//! println!("{}", toml);
//! ```
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 12 — RunSpec XML parser
//! * Task 13 — TOML-based RunSpec format
//! * Task 14 — Pollutant/process/source-type/road-type enums (will replace
//!   the `(id, name)` pairs in pollutant/process/road-type/fuel-type
//!   selections once the canonical lookup tables land in `moves-data`).

pub mod error;
pub mod model;
pub mod toml_format;
pub mod xml_format;

pub use error::{Error, Result};
pub use model::*;

/// Parse a TOML RunSpec string into the canonical [`RunSpec`] model.
pub fn from_toml_str(input: &str) -> Result<RunSpec> {
    toml_format::parse(input)
}

/// Serialize a [`RunSpec`] back to a TOML string.
///
/// The output is reparseable into a model-equivalent value; see
/// `tests/round_trip.rs` for the contract.
pub fn to_toml_string(spec: &RunSpec) -> Result<String> {
    toml_format::to_string(spec)
}

/// Parse an XML RunSpec string into the canonical [`RunSpec`] model.
pub fn from_xml_str(input: &str) -> Result<RunSpec> {
    xml_format::parse(input)
}

/// Serialize a [`RunSpec`] back to an XML string.
///
/// The output is reparseable into a model-equivalent value. Byte-identical
/// re-serialization (whitespace, CDATA wrapping, attribute order) is a
/// non-goal here — Task 12 will harden that contract.
pub fn to_xml_string(spec: &RunSpec) -> Result<String> {
    xml_format::to_string(spec)
}

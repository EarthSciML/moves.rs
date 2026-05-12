//! Error types for the `moves-runspec` crate.

use thiserror::Error;

/// Convenience alias for `Result` with this crate's error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced when parsing or serializing a MOVES RunSpec.
#[derive(Debug, Error)]
pub enum Error {
    /// Low-level XML parse failure from `quick-xml`.
    #[error("XML parse error: {0}")]
    Xml(#[from] quick_xml::Error),

    /// XML attribute decoding failure from `quick-xml`.
    #[error("XML attribute error: {0}")]
    XmlAttr(#[from] quick_xml::events::attributes::AttrError),

    /// The document's root element was not `<runspec>`.
    #[error("expected <runspec> root element, found <{0}>")]
    RootMismatch(String),

    /// Integer attribute could not be parsed.
    #[error("invalid integer in attribute `{attr}` on <{element}>: {value}")]
    InvalidInt {
        /// Element name where the bad attribute appeared.
        element: String,
        /// Attribute name.
        attr: String,
        /// Raw attribute value.
        value: String,
    },

    /// Floating-point attribute could not be parsed.
    #[error("invalid float in attribute `{attr}` on <{element}>: {value}")]
    InvalidFloat {
        /// Element name where the bad attribute appeared.
        element: String,
        /// Attribute name.
        attr: String,
        /// Raw attribute value.
        value: String,
    },

    /// Enum value did not match a known variant.
    #[error("unknown {kind} value `{value}` on <{element}>")]
    UnknownEnumValue {
        /// Element name where the bad value appeared.
        element: String,
        /// Logical kind of enum (e.g. `ModelScale`).
        kind: &'static str,
        /// Raw value.
        value: String,
    },

    /// I/O failure while writing serialized XML to a target writer.
    #[error("XML write error: {0}")]
    Io(#[from] std::io::Error),

    /// Formatting failure when building XML in memory.
    #[error("XML format error: {0}")]
    Fmt(#[from] std::fmt::Error),

    /// Generic structural problem with the XML document.
    #[error("malformed RunSpec XML: {0}")]
    Malformed(String),
}

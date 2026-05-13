//! Error type for `moves-runspec`.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("toml deserialize: {0}")]
    TomlDeserialize(#[from] toml::de::Error),

    #[error("toml serialize: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("xml: {0}")]
    Xml(#[from] quick_xml::DeError),

    #[error("xml write: {0}")]
    XmlWrite(String),

    #[error("invalid enum value for {field}: {value:?}")]
    InvalidEnumValue { field: &'static str, value: String },
}

impl Error {
    pub(crate) fn from_fmt(e: std::fmt::Error) -> Self {
        Error::XmlWrite(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

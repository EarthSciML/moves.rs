//! Minimal RunSpec XML parser for the fields fixture-capture needs.
//!
//! This is **not** the full RunSpec parser — that arrives in Phase 2 (Task 12)
//! as the `moves-runspec` crate. Here we only extract:
//! * `<outputdatabase databasename="...">` — the database MOVES writes to
//! * `<scaleinputdatabase databasename="...">` — the default-DB name (which
//!   we exclude from capture, since it isn't modified by the run)
//! * a stable fixture identifier derived from the RunSpec file name
//!
//! Anything beyond those fields is ignored.

use std::path::{Path, PathBuf};

use quick_xml::events::{BytesStart, Event};
use quick_xml::reader::Reader;

use crate::error::{Error, Result};

/// The slice of a MOVES RunSpec that fixture-capture needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSpec {
    /// Filename-derived identifier (e.g., `samplerunspec` for `SampleRunSpec.xml`).
    pub fixture_name: String,
    /// Source path the RunSpec was parsed from.
    pub path: PathBuf,
    /// Name of the DB MOVES writes its output to. Required.
    pub output_database: String,
    /// Name of the default-input (scale) DB. Optional — older RunSpecs use a
    /// different element name. None = "no default-input DB declared, capture
    /// every non-system DB".
    pub scale_input_database: Option<String>,
}

impl RunSpec {
    /// Parse the RunSpec at `path`, returning the fields fixture-capture needs.
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_bytes(path, &bytes)
    }

    /// Parse a RunSpec from in-memory bytes. `path` is used for diagnostic
    /// messages and to derive the fixture name.
    pub fn from_bytes(path: &Path, bytes: &[u8]) -> Result<Self> {
        let fixture_name = derive_fixture_name(path);

        let mut reader = Reader::from_reader(bytes);
        reader.config_mut().trim_text(true);

        let mut output_database: Option<String> = None;
        let mut scale_input_database: Option<String> = None;
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Empty(e)) | Ok(Event::Start(e)) => {
                    let tag = std::str::from_utf8(e.name().as_ref())
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    match tag.as_str() {
                        "outputdatabase" => {
                            if let Some(v) = attr(&e, "databasename")? {
                                output_database = Some(v);
                            }
                        }
                        "scaleinputdatabase" | "defaultdatabase" => {
                            if let Some(v) = attr(&e, "databasename")? {
                                scale_input_database = Some(v);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(source) => {
                    return Err(Error::Xml {
                        path: path.to_path_buf(),
                        source,
                    });
                }
                _ => {}
            }
            buf.clear();
        }

        let output_database =
            output_database
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::RunSpecMissing {
                    path: path.to_path_buf(),
                    element: "outputdatabase[@databasename]".to_string(),
                })?;

        let scale_input_database = scale_input_database.filter(|s| !s.is_empty());

        Ok(Self {
            fixture_name,
            path: path.to_path_buf(),
            output_database,
            scale_input_database,
        })
    }

    /// Returns true if `db_name` is the scale-input (default) DB declared by
    /// the RunSpec. Used to filter the database list before capture — the
    /// default DB is read-only during a run, so dumping it would just bloat
    /// the snapshot with content already pinned by the SIF SHA.
    pub fn is_default_db(&self, db_name: &str) -> bool {
        self.scale_input_database
            .as_deref()
            .map(|d| d.eq_ignore_ascii_case(db_name))
            .unwrap_or(false)
    }
}

fn attr(e: &BytesStart, name: &str) -> Result<Option<String>> {
    for attr in e.attributes() {
        let attr = attr.map_err(|source| Error::Xml {
            path: PathBuf::from("<runspec>"),
            source: source.into(),
        })?;
        let key = std::str::from_utf8(attr.key.as_ref()).unwrap_or("");
        if key.eq_ignore_ascii_case(name) {
            // The XML attribute set in MOVES RunSpecs is plain ASCII (no
            // entity-escaped values worth worrying about), but unescape via
            // the library to be defensive.
            let value = attr.unescape_value().map_err(|source| Error::Xml {
                path: PathBuf::from("<runspec>"),
                source,
            })?;
            return Ok(Some(value.into_owned()));
        }
    }
    Ok(None)
}

fn derive_fixture_name(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unnamed");
    let mut out = String::with_capacity(stem.len());
    for ch in stem.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("unnamed");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const SAMPLE_RUNSPEC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<runspec version="MOVES5">
  <description>Sample RunSpec</description>
  <scaleinputdatabase servername="" databasename="movesdb20241112" description=""/>
  <outputdatabase servername="" databasename="movesoutput" description=""/>
</runspec>
"#;

    #[test]
    fn parses_sample_runspec() {
        let rs = RunSpec::from_bytes(Path::new("SampleRunSpec.xml"), SAMPLE_RUNSPEC.as_bytes())
            .expect("parse");
        assert_eq!(rs.fixture_name, "samplerunspec");
        assert_eq!(rs.output_database, "movesoutput");
        assert_eq!(rs.scale_input_database.as_deref(), Some("movesdb20241112"));
        assert!(rs.is_default_db("movesdb20241112"));
        assert!(rs.is_default_db("MOVESDB20241112"));
        assert!(!rs.is_default_db("movesoutput"));
    }

    #[test]
    fn missing_output_database_is_error() {
        let xml = r#"<runspec><scaleinputdatabase databasename="x"/></runspec>"#;
        let err = RunSpec::from_bytes(Path::new("t.xml"), xml.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::RunSpecMissing { .. }));
    }

    #[test]
    fn empty_databasename_treated_as_missing() {
        let xml = r#"<runspec><outputdatabase databasename=""/></runspec>"#;
        let err = RunSpec::from_bytes(Path::new("t.xml"), xml.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::RunSpecMissing { .. }));
    }

    #[test]
    fn empty_scale_input_treated_as_missing() {
        let xml = r#"<runspec>
            <scaleinputdatabase databasename=""/>
            <outputdatabase databasename="movesoutput"/>
        </runspec>"#;
        let rs = RunSpec::from_bytes(Path::new("t.xml"), xml.as_bytes()).unwrap();
        assert_eq!(rs.scale_input_database, None);
    }

    #[test]
    fn fixture_name_sanitizes_filename() {
        let path = PathBuf::from("/x/Phase 0 Spec.XML");
        assert_eq!(derive_fixture_name(&path), "phase_0_spec");

        let path = PathBuf::from("a-b_c-d.xml");
        assert_eq!(derive_fixture_name(&path), "a-b_c-d");

        let path = PathBuf::from("");
        assert_eq!(derive_fixture_name(&path), "unnamed");
    }

    #[test]
    fn handles_self_closing_and_open_close_forms() {
        // Self-closing — the form MOVES emits.
        let xml1 = r#"<runspec><outputdatabase databasename="o"/></runspec>"#;
        let rs1 = RunSpec::from_bytes(Path::new("t.xml"), xml1.as_bytes()).unwrap();
        assert_eq!(rs1.output_database, "o");

        // Open/close — defensive, in case a hand-edited RunSpec uses it.
        let xml2 = r#"<runspec><outputdatabase databasename="o"></outputdatabase></runspec>"#;
        let rs2 = RunSpec::from_bytes(Path::new("t.xml"), xml2.as_bytes()).unwrap();
        assert_eq!(rs2.output_database, "o");
    }

    #[test]
    fn case_insensitive_attribute_names() {
        let xml = r#"<runspec><outputdatabase DataBaseName="o"/></runspec>"#;
        let rs = RunSpec::from_bytes(Path::new("t.xml"), xml.as_bytes()).unwrap();
        assert_eq!(rs.output_database, "o");
    }

    #[test]
    fn case_insensitive_element_names() {
        let xml = r#"<RunSpec><OutputDatabase databasename="o"/></RunSpec>"#;
        let rs = RunSpec::from_bytes(Path::new("t.xml"), xml.as_bytes()).unwrap();
        assert_eq!(rs.output_database, "o");
    }
}

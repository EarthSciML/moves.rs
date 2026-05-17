//! The Phase 0 NONROAD fixture catalogue.
//!
//! Phase 0 (`characterization/fixtures/README.md`) ships ten
//! `nr-*.xml` RunSpec fixtures — one NONROAD model run per
//! equipment sector and geography level. Task 115's bead is to run
//! *all* of them through the Rust port and diff each against the
//! gfortran reference.
//!
//! This module is the registry side of that: it enumerates the ten
//! fixtures, locates them under the repository's `characterization/`
//! tree, and does a light parse of each RunSpec so the harness and
//! the divergence reports can name the geography level and year a
//! fixture exercises. Each fixture's reference capture is expected
//! at `<name>.tsv` (see [`NonroadFixture::reference_filename`]).

use std::path::{Path, PathBuf};

/// The ten Phase 0 NONROAD fixture names, matching the `nr-*.xml`
/// files in `characterization/fixtures/`. The fixture *name* is the
/// file stem (see `characterization/fixtures/README.md` § "Naming
/// conventions").
pub const FIXTURE_NAMES: &[&str] = &[
    "nr-agriculture-state",
    "nr-airport-support-county",
    "nr-commercial-nation",
    "nr-construction-state",
    "nr-industrial-county",
    "nr-lawn-garden-county",
    "nr-logging-county",
    "nr-pleasure-craft-state",
    "nr-railroad-support-nation",
    "nr-recreational-county",
];

/// The repository root, derived from the crate's manifest directory.
///
/// `CARGO_MANIFEST_DIR` is `<repo>/crates/moves-nonroad`; its
/// grandparent is the repository root.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate manifest dir has a repo-root grandparent")
        .to_path_buf()
}

/// The Phase 0 fixture directory: `characterization/fixtures/`.
pub fn fixtures_dir() -> PathBuf {
    repo_root().join("characterization").join("fixtures")
}

/// A NONROAD fixture: its name, on-disk path, and the run dimensions
/// scraped from the RunSpec XML.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NonroadFixture {
    /// The fixture name (file stem, e.g. `nr-construction-state`).
    pub name: String,
    /// Absolute path to the RunSpec XML.
    pub path: PathBuf,
    /// `true` when the RunSpec selects the NONROAD model.
    pub is_nonroad: bool,
    /// Geography level — `STATE`, `COUNTY`, or `NATION` — from the
    /// `<geographicselection type="…">` element.
    pub geography_level: Option<String>,
    /// Calendar year from the RunSpec `<timespan>`.
    pub year: Option<i32>,
    /// The RunSpec `<description>` CDATA text, if present.
    pub description: Option<String>,
}

impl NonroadFixture {
    /// The expected filename of this fixture's gfortran reference
    /// capture within the directory named by the
    /// `NONROAD_FIDELITY_REFERENCE` environment variable.
    pub fn reference_filename(&self) -> String {
        format!("{}.tsv", self.name)
    }
}

/// Pull the value of `attr` from the first XML element whose opening
/// tag begins with `element` (which must include a trailing space so
/// `"<model "` does not also match the `<models>` container).
fn attr_of(xml: &str, element: &str, attr: &str) -> Option<String> {
    let start = xml.find(element)?;
    let rest = &xml[start..];
    let tag_end = rest.find('>')?;
    let tag = &rest[..tag_end];
    let needle = format!("{attr}=\"");
    let value_start = tag.find(&needle)? + needle.len();
    let value_len = tag[value_start..].find('"')?;
    Some(tag[value_start..value_start + value_len].to_string())
}

/// Extract the first `<![CDATA[ … ]]>` block's text.
fn cdata_of(xml: &str) -> Option<String> {
    let open = "<![CDATA[";
    let start = xml.find(open)? + open.len();
    let len = xml[start..].find("]]>")?;
    Some(xml[start..start + len].trim().to_string())
}

/// Load and light-parse one fixture by name.
///
/// # Errors
///
/// Returns an [`std::io::Error`] when the RunSpec file is missing or
/// unreadable.
pub fn load_fixture(name: &str) -> std::io::Result<NonroadFixture> {
    let path = fixtures_dir().join(format!("{name}.xml"));
    let xml = std::fs::read_to_string(&path)?;
    let is_nonroad = attr_of(&xml, "<model ", "value").as_deref() == Some("NONROAD");
    let geography_level = attr_of(&xml, "<geographicselection ", "type");
    let year = attr_of(&xml, "<year ", "key").and_then(|y| y.parse().ok());
    let description = cdata_of(&xml);
    Ok(NonroadFixture {
        name: name.to_string(),
        path,
        is_nonroad,
        geography_level,
        year,
        description,
    })
}

/// Load all ten Phase 0 NONROAD fixtures, in [`FIXTURE_NAMES`] order.
///
/// # Errors
///
/// Returns the first [`std::io::Error`] encountered.
pub fn load_all_fixtures() -> std::io::Result<Vec<NonroadFixture>> {
    FIXTURE_NAMES
        .iter()
        .map(|name| load_fixture(name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_are_ten_fixtures_with_unique_names() {
        assert_eq!(FIXTURE_NAMES.len(), 10);
        let mut sorted = FIXTURE_NAMES.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 10, "fixture names must be unique");
    }

    #[test]
    fn fixtures_dir_resolves_under_the_repo() {
        let dir = fixtures_dir();
        assert!(
            dir.ends_with("characterization/fixtures"),
            "unexpected fixtures dir: {}",
            dir.display()
        );
    }

    #[test]
    fn attr_of_skips_the_plural_container_element() {
        let xml = r#"<models><model value="NONROAD"/></models>"#;
        // "<model " (trailing space) must not match "<models>".
        assert_eq!(attr_of(xml, "<model ", "value").as_deref(), Some("NONROAD"));
        let geo = r#"<geographicselections><geographicselection type="STATE" key="26"/>"#;
        assert_eq!(
            attr_of(geo, "<geographicselection ", "type").as_deref(),
            Some("STATE")
        );
    }

    #[test]
    fn cdata_of_extracts_description_text() {
        let xml = "<description><![CDATA[ NONROAD Construction sector. ]]></description>";
        assert_eq!(
            cdata_of(xml).as_deref(),
            Some("NONROAD Construction sector.")
        );
        assert_eq!(cdata_of("<description></description>"), None);
    }

    #[test]
    fn reference_filename_is_the_name_with_tsv_suffix() {
        let fixture = NonroadFixture {
            name: "nr-construction-state".to_string(),
            path: PathBuf::new(),
            is_nonroad: true,
            geography_level: Some("STATE".to_string()),
            year: Some(2020),
            description: None,
        };
        assert_eq!(fixture.reference_filename(), "nr-construction-state.tsv");
    }
}

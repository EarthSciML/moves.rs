//! The onroad fixture catalogue.
//!
//! (`characterization/fixtures/README.md`) ships 33 RunSpec
//! fixtures. Twenty-three select the **onroad** model — the runs that
//! exercise the generators this harness validates; the other
//! ten are `nr-*.xml` NONROAD fixtures owned by the gate
//! (see the module-level docs for the scope split).
//!
//! This module enumerates the 23 onroad fixtures, locates them under
//! the repository's `characterization/` tree, and parses each RunSpec
//! through [`moves_runspec`] so the harness can name the model scale,
//! domain, year, and — crucially for [`super::coverage`] — the
//! emission processes each fixture exercises.

use std::fmt;
use std::path::PathBuf;

use moves_runspec::{from_xml_str, Model, ModelDomain, ModelScale};

use super::repo_root;

/// The 23 onroad fixture names — the file stems of the
/// non-`nr-*` RunSpec XMLs in `characterization/fixtures/`.
///
/// Ordered the way `characterization/fixtures/coverage-matrix.md`
/// lists them: the canonical sample, its one-dimension expansions,
/// the process-focal fixtures, the chain-leaf fixture, the
/// scale/domain fixtures.
pub const ONROAD_FIXTURE_NAMES: &[&str] = &[
    "sample-runspec",
    "expand-day",
    "expand-month",
    "expand-counties",
    "expand-fueltype-diesel",
    "expand-sourcetype",
    "expand-criteria",
    "process-brakewear",
    "process-tirewear",
    "process-pm-exhaust",
    "process-evap-permeation",
    "process-evap-fvv",
    "process-evap-leaks",
    "process-refueling",
    "process-crankcase-running",
    "process-crankcase-start",
    "process-crankcase-extidle",
    "process-apu",
    "process-airtoxics",
    "chain-tog-speciation",
    "scale-county",
    "scale-project",
    "scale-rates",
];

/// The fixture directory: `characterization/fixtures/`.
pub fn fixtures_dir() -> PathBuf {
    repo_root().join("characterization").join("fixtures")
}

/// A failure loading or parsing one fixture RunSpec.
#[derive(Debug)]
pub enum FixtureError {
 /// The RunSpec XML file is missing or unreadable.
    Io {
 /// Fixture name the failure belongs to.
        name: String,
 /// The underlying I/O error.
        source: std::io::Error,
    },
 /// The RunSpec XML failed to parse through `moves-runspec`.
    Parse {
 /// Fixture name the failure belongs to.
        name: String,
 /// The underlying parser error.
        source: moves_runspec::Error,
    },
}

impl fmt::Display for FixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FixtureError::Io { name, source } => {
                write!(f, "fixture `{name}`: cannot read RunSpec XML: {source}")
            }
            FixtureError::Parse { name, source } => {
                write!(f, "fixture `{name}`: RunSpec XML did not parse: {source}")
            }
        }
    }
}

impl std::error::Error for FixtureError {}

/// An onroad fixture: its name, on-disk path, and the run dimensions
/// parsed from the RunSpec XML.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnroadFixture {
 /// The fixture name (file stem, e.g. `process-brakewear`).
    pub name: String,
 /// Absolute path to the RunSpec XML.
    pub path: PathBuf,
 /// `true` when the RunSpec selects no NONROAD model — i.e. every
 /// `<model>` is ONROAD, or the legacy RunSpec omits `<models>`
 /// entirely (the canonical `SampleRunSpec.xml` predates the
 /// element). Every fixture in [`ONROAD_FIXTURE_NAMES`] satisfies it.
    pub is_onroad: bool,
 /// `<modelscale>` — Macro, Inventory, or Rates.
    pub scale: ModelScale,
 /// `<modeldomain>` — Default, Single (county), or Project.
    pub domain: Option<ModelDomain>,
 /// First `<timespan>` calendar year, if the RunSpec records one.
    pub year: Option<u32>,
 /// Distinct `emissionprocess.processID` values the RunSpec's
 /// `<pollutantprocessassociations>` exercise, ascending. This is
 /// the key the coverage matrix joins generator subscriptions on.
    pub process_ids: Vec<u32>,
 /// The RunSpec `<description>` CDATA text, if present.
    pub description: Option<String>,
}

/// Extract the first `<![CDATA[ … ]]>` block's trimmed text. The
/// `RunSpec` model does not carry the description, so the harness
/// scrapes it straight from the XML for the rendered coverage matrix.
fn cdata_of(xml: &str) -> Option<String> {
    let open = "<![CDATA[";
    let start = xml.find(open)? + open.len();
    let len = xml[start..].find("]]>")?;
    let text = xml[start..start + len].trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Load and parse one onroad fixture by name.
///
/// # Errors
///
/// [`FixtureError::Io`] when the RunSpec file is missing or
/// unreadable; [`FixtureError::Parse`] when `moves-runspec` rejects
/// the XML.
pub fn load_fixture(name: &str) -> Result<OnroadFixture, FixtureError> {
    let path = fixtures_dir().join(format!("{name}.xml"));
    let xml = std::fs::read_to_string(&path).map_err(|source| FixtureError::Io {
        name: name.to_string(),
        source,
    })?;
    let spec = from_xml_str(&xml).map_err(|source| FixtureError::Parse {
        name: name.to_string(),
        source,
    })?;

 // A RunSpec with no `<models>` element is implicitly ONROAD — the
 // element is a newer MOVES addition and the canonical
 // `SampleRunSpec.xml` omits it. `all` is vacuously true on an empty
 // model list, which correctly classifies that case as onroad; a
 // `<model value="NONROAD"/>` is what flips it false.
    let is_onroad = spec.models.iter().all(|m| *m == Model::Onroad);

    let mut process_ids: Vec<u32> = spec
        .pollutant_process_associations
        .iter()
        .map(|ppa| ppa.process_id)
        .collect();
    process_ids.sort_unstable();
    process_ids.dedup();

    Ok(OnroadFixture {
        name: name.to_string(),
        path,
        is_onroad,
        scale: spec.scale,
        domain: spec.domain,
        year: spec.timespan.years.first().copied(),
        process_ids,
        description: cdata_of(&xml),
    })
}

/// Load all 23 onroad fixtures, in [`ONROAD_FIXTURE_NAMES`] order.
///
/// # Errors
///
/// Returns the first [`FixtureError`] encountered.
pub fn load_all_fixtures() -> Result<Vec<OnroadFixture>, FixtureError> {
    ONROAD_FIXTURE_NAMES
        .iter()
        .map(|n| load_fixture(n))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_has_23_unique_names() {
        assert_eq!(ONROAD_FIXTURE_NAMES.len(), 23);
        let mut sorted = ONROAD_FIXTURE_NAMES.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 23, "fixture names must be unique");
    }

    #[test]
    fn no_nonroad_fixtures_in_the_onroad_catalogue() {
 // The `nr-*` fixtures belong to the NONROAD gate.
        assert!(
            ONROAD_FIXTURE_NAMES.iter().all(|n| !n.starts_with("nr-")),
            "an nr-* NONROAD fixture leaked into the onroad catalogue"
        );
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
    fn cdata_of_extracts_and_trims_description() {
        let xml = "<description><![CDATA[ Brakewear PM10 + PM2.5. ]]></description>";
        assert_eq!(cdata_of(xml).as_deref(), Some("Brakewear PM10 + PM2.5."));
        assert_eq!(cdata_of("<description></description>"), None);
        assert_eq!(cdata_of("<description><![CDATA[   ]]></description>"), None);
    }

    #[test]
    fn loaded_fixtures_expose_parsed_run_dimensions() {
        let fixtures = load_all_fixtures().expect("the 23 onroad fixtures must load");
        assert_eq!(fixtures.len(), 23);

        for fixture in &fixtures {
            assert!(fixture.is_onroad, "{} is not ONROAD", fixture.name);
            assert!(
                fixture.year.is_some(),
                "{} has no timespan year",
                fixture.name
            );
            assert!(
                !fixture.process_ids.is_empty(),
                "{} exercises no emission process",
                fixture.name
            );
        }

 // The scale/domain fixtures are named for the dimension they pin.
        let rates = fixtures
            .iter()
            .find(|f| f.name == "scale-rates")
            .expect("scale-rates fixture present");
        assert_eq!(
            rates.scale,
            ModelScale::Rates,
            "scale-rates must be Rates scale"
        );

        let project = fixtures
            .iter()
            .find(|f| f.name == "scale-project")
            .expect("scale-project fixture present");
        assert_eq!(
            project.domain,
            Some(ModelDomain::Project),
            "scale-project must be Project domain"
        );

 // Every fixture is authored with a <description> CDATA block.
        let described = fixtures.iter().filter(|f| f.description.is_some()).count();
        assert!(
            described >= 20,
            "expected most fixtures to carry a description, got {described}"
        );
    }
}

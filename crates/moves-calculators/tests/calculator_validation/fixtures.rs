//! The Phase 0 onroad fixture catalogue.
//!
//! Phase 0 (`characterization/fixtures/README.md`) ships 33 RunSpec
//! fixtures. Twenty-three select the **onroad** model — the runs that
//! exercise the Phase 3 calculators this harness validates; the other
//! ten are `nr-*.xml` NONROAD fixtures owned by the Task 115 gate.
//!
//! Task 74 (`mo-wkjj`) adds three more onroad fixtures that cover the
//! four calculators the original 23 hot-path fixtures left uncovered:
//! `process-nox-speciation` (NOCalculator, NO2Calculator),
//! `process-extended-idle` (CO2AERunningStartExtendedIdleCalculator),
//! and `chain-nonhaptog` (TogSpeciationCalculator). The full catalogue
//! is now 26 onroad fixtures.
//!
//! This module enumerates the 26 onroad fixtures, locates them under
//! the repository's `characterization/` tree, and parses each RunSpec
//! through [`moves_runspec`] so the harness can name the model scale,
//! domain, year, and — crucially for [`super::coverage`] — the
//! (pollutant, process) pairs each fixture exercises.

use std::fmt;
use std::path::PathBuf;

use moves_runspec::{from_xml_str, Model, ModelDomain, ModelScale};

use super::repo_root;

/// The 26 onroad fixture names — the file stems of the non-`nr-*`
/// RunSpec XMLs in `characterization/fixtures/`.
///
/// The original 23 Phase 0 hot-path fixtures are listed first; the
/// three Task 74 (`mo-wkjj`) fixtures that cover the previously
/// uncovered calculators follow.
pub const ONROAD_FIXTURE_NAMES: &[&str] = &[
    // Phase 0 hot-path fixtures (23)
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
    // Task 74 fixtures — cover the four previously uncovered calculators (3)
    "process-nox-speciation",   // NOCalculator (32,1), NO2Calculator (33,1)
    "process-extended-idle",    // CO2AERunningStartExtendedIdleCalculator (90,90)
    "chain-nonhaptog",          // TogSpeciationCalculator (88,1)
];

/// The Phase 0 fixture directory: `characterization/fixtures/`.
pub fn fixtures_dir() -> PathBuf {
    repo_root().join("characterization").join("fixtures")
}

/// A failure loading or parsing one fixture RunSpec.
#[derive(Debug)]
pub enum FixtureError {
    Io {
        name: String,
        source: std::io::Error,
    },
    Parse {
        name: String,
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
    /// `true` when the RunSpec selects no NONROAD model.
    pub is_onroad: bool,
    /// `<modelscale>`.
    pub scale: ModelScale,
    /// `<modeldomain>`.
    pub domain: Option<ModelDomain>,
    /// First `<timespan>` calendar year, if the RunSpec records one.
    pub year: Option<u32>,
    /// Distinct process IDs the fixture exercises, ascending.
    /// Subset of `ppa_ids` — retained for compatibility with the
    /// process-based coverage logic.
    pub process_ids: Vec<u32>,
    /// Distinct `(pollutant_id, process_id)` pairs the fixture exercises,
    /// sorted by (process_id, pollutant_id). This is the key the coverage
    /// matrix joins calculator registrations on.
    pub ppa_ids: Vec<(u32, u32)>,
    /// The RunSpec `<description>` CDATA text, if present.
    pub description: Option<String>,
}

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

    let is_onroad = spec.models.iter().all(|m| *m == Model::Onroad);

    let mut process_ids: Vec<u32> = spec
        .pollutant_process_associations
        .iter()
        .map(|ppa| ppa.process_id)
        .collect();
    process_ids.sort_unstable();
    process_ids.dedup();

    let mut ppa_ids: Vec<(u32, u32)> = spec
        .pollutant_process_associations
        .iter()
        .map(|ppa| (ppa.pollutant_id, ppa.process_id))
        .collect();
    ppa_ids.sort_unstable();
    ppa_ids.dedup();

    Ok(OnroadFixture {
        name: name.to_string(),
        path,
        is_onroad,
        scale: spec.scale,
        domain: spec.domain,
        year: spec.timespan.years.first().copied(),
        process_ids,
        ppa_ids,
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
    fn the_catalogue_has_26_unique_names() {
        assert_eq!(ONROAD_FIXTURE_NAMES.len(), 26);
        let mut sorted = ONROAD_FIXTURE_NAMES.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 26, "fixture names must be unique");
    }

    #[test]
    fn no_nonroad_fixtures_in_the_onroad_catalogue() {
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
    fn loaded_fixtures_expose_parsed_run_dimensions() {
        let fixtures = load_all_fixtures().expect("the 26 onroad fixtures must load");
        assert_eq!(fixtures.len(), 26);

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
            assert!(
                !fixture.ppa_ids.is_empty(),
                "{} has no (pollutant, process) pairs",
                fixture.name
            );
            // ppa_ids always at least as large as process_ids
            assert!(fixture.ppa_ids.len() >= fixture.process_ids.len());
        }
    }

    #[test]
    fn ppa_ids_are_sorted_and_deduplicated() {
        let fixtures = load_all_fixtures().expect("the 26 onroad fixtures must load");
        for fixture in &fixtures {
            let mut sorted = fixture.ppa_ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                fixture.ppa_ids, sorted,
                "{}: ppa_ids must be sorted and deduplicated",
                fixture.name
            );
        }
    }
}

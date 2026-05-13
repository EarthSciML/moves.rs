//! Round-trip integration tests for the `moves-runspec` TOML and XML
//! formats.
//!
//! What we verify:
//!
//! * **TOML self-loop** — `RunSpec → TOML → RunSpec` is model-identical.
//! * **XML self-loop** — `RunSpec → XML → RunSpec` is model-identical.
//! * **XML serialization is idempotent** — `serialize → parse → serialize`
//!   is byte-identical (Task 12's byte-stable contract).
//! * **Cross-format** — `XML → RunSpec → TOML → RunSpec → XML → RunSpec`
//!   collapses to a single model value, so the two surfaces really are
//!   isomorphic (Task 13's central promise).
//! * **Fixture sweep** — every fixture in `characterization/fixtures/`
//!   parses, re-serializes, and round-trips through the model. Each
//!   fixture is exercised as a separate test so failures point at the
//!   specific RunSpec that broke.
//! * **Java parity** — the values from `RunSpecTest.testSampleValues`
//!   (the canonical Java unit test for `sample-runspec.xml`) are asserted
//!   against the parsed model, and an in-code-built RunSpec is round-tripped
//!   through XML to verify the build-test-parse path the Java equivalents
//!   exercise.

use std::path::{Path, PathBuf};

use moves_runspec::{
    from_toml_str, from_xml_str, to_toml_string, to_xml_string, DatabaseRef, DistanceUnit,
    EnergyUnit, GeoKind, GeographicOutputDetail, GeographicSelection, MassUnit, Model, ModelScale,
    OnroadVehicleSelection, OutputTimestep, PollutantProcessAssociation, RoadType, RunSpec,
    TimeUnit,
};

fn workspace_root() -> PathBuf {
    // tests run with CARGO_MANIFEST_DIR=crates/moves-runspec
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(Path::parent)
        .expect("workspace root above crates/moves-runspec/")
        .to_path_buf()
}

fn fixture(rel: &str) -> PathBuf {
    workspace_root().join(rel)
}

fn fixtures_dir() -> PathBuf {
    workspace_root().join("characterization/fixtures")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn xml_self_round_trip_sample() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let spec = from_xml_str(&xml).expect("parse sample-runspec.xml");
    let serialized = to_xml_string(&spec).expect("serialize XML");
    let reparsed = from_xml_str(&serialized).expect("reparse serialized XML");
    assert_eq!(
        spec, reparsed,
        "XML → model → XML → model must be model-identical"
    );
}

#[test]
fn toml_self_round_trip_sample() {
    let toml = read(&fixture(
        "crates/moves-runspec/tests/fixtures/sample-runspec.toml",
    ));
    let spec = from_toml_str(&toml).expect("parse sample-runspec.toml");
    let serialized = to_toml_string(&spec).expect("serialize TOML");
    let reparsed = from_toml_str(&serialized).expect("reparse serialized TOML");
    assert_eq!(
        spec, reparsed,
        "TOML → model → TOML → model must be model-identical"
    );
}

#[test]
fn hand_authored_toml_matches_xml_fixture() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let toml = read(&fixture(
        "crates/moves-runspec/tests/fixtures/sample-runspec.toml",
    ));
    let from_xml = from_xml_str(&xml).expect("parse XML");
    let from_toml = from_toml_str(&toml).expect("parse TOML");
    assert_eq!(
        from_xml, from_toml,
        "the hand-authored TOML and the canonical XML must produce the same model"
    );
}

#[test]
fn cross_format_round_trip_sample() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let from_xml = from_xml_str(&xml).expect("parse XML");
    let toml = to_toml_string(&from_xml).expect("serialize TOML");
    let from_toml = from_toml_str(&toml).expect("parse the serialized TOML");
    let xml2 = to_xml_string(&from_toml).expect("serialize XML");
    let from_xml2 = from_xml_str(&xml2).expect("parse the serialized XML");
    assert_eq!(
        from_xml, from_toml,
        "XML → model → TOML → model must be model-identical"
    );
    assert_eq!(
        from_xml, from_xml2,
        "XML → model → TOML → model → XML → model must collapse to one model"
    );
}

#[test]
fn unknown_enum_value_is_an_error() {
    let mut xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    xml = xml.replace("MACROSCALE", "NotAScale");
    let err = from_xml_str(&xml).expect_err("invalid modelscale must error");
    let msg = err.to_string();
    assert!(
        msg.contains("modelscale.value") && msg.contains("NotAScale"),
        "error should name the bad field+value: {msg}"
    );
}

// --- Per-fixture coverage -----------------------------------------------
//
// The 33 fixtures under `characterization/fixtures/` collectively exercise
// every RunSpec element type the 23 Java files in `gov.epa.otaq.moves.master
// .runspec` know about. Each fixture is its own test so failures point at
// the offending RunSpec rather than a generic sweep failure.

/// Round-trip a single fixture through XML: parse → serialize → parse
/// (model equivalence) and serialize → parse → serialize (byte stability).
fn round_trip_fixture(name: &str) {
    let path = fixtures_dir().join(name);
    let xml = read(&path);
    let spec = from_xml_str(&xml).unwrap_or_else(|e| panic!("parse {name}: {e}"));

    // Self-loop is model-identical.
    let out1 = to_xml_string(&spec).expect("serialize");
    let reparsed = from_xml_str(&out1).unwrap_or_else(|e| panic!("reparse {name}: {e}"));
    assert_eq!(spec, reparsed, "XML self-loop failed for {name}");

    // Serializer is idempotent: the second round-trip is byte-identical.
    let out2 = to_xml_string(&reparsed).expect("re-serialize");
    assert_eq!(out1, out2, "serializer not idempotent on {name}");

    // Cross-format: model survives a TOML detour too.
    let toml = to_toml_string(&spec).expect("serialize TOML");
    let from_toml = from_toml_str(&toml).unwrap_or_else(|e| panic!("re-parse TOML {name}: {e}"));
    assert_eq!(spec, from_toml, "cross-format round-trip failed for {name}");
}

macro_rules! per_fixture_round_trip {
    ($($name:ident => $file:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                round_trip_fixture($file);
            }
        )*
    };
}

per_fixture_round_trip! {
    fixture_sample_runspec => "sample-runspec.xml",
    fixture_chain_tog_speciation => "chain-tog-speciation.xml",
    fixture_expand_counties => "expand-counties.xml",
    fixture_expand_criteria => "expand-criteria.xml",
    fixture_expand_day => "expand-day.xml",
    fixture_expand_fueltype_diesel => "expand-fueltype-diesel.xml",
    fixture_expand_month => "expand-month.xml",
    fixture_expand_sourcetype => "expand-sourcetype.xml",
    fixture_nr_agriculture_state => "nr-agriculture-state.xml",
    fixture_nr_airport_support_county => "nr-airport-support-county.xml",
    fixture_nr_commercial_nation => "nr-commercial-nation.xml",
    fixture_nr_construction_state => "nr-construction-state.xml",
    fixture_nr_industrial_county => "nr-industrial-county.xml",
    fixture_nr_lawn_garden_county => "nr-lawn-garden-county.xml",
    fixture_nr_logging_county => "nr-logging-county.xml",
    fixture_nr_pleasure_craft_state => "nr-pleasure-craft-state.xml",
    fixture_nr_railroad_support_nation => "nr-railroad-support-nation.xml",
    fixture_nr_recreational_county => "nr-recreational-county.xml",
    fixture_process_airtoxics => "process-airtoxics.xml",
    fixture_process_apu => "process-apu.xml",
    fixture_process_brakewear => "process-brakewear.xml",
    fixture_process_crankcase_extidle => "process-crankcase-extidle.xml",
    fixture_process_crankcase_running => "process-crankcase-running.xml",
    fixture_process_crankcase_start => "process-crankcase-start.xml",
    fixture_process_evap_fvv => "process-evap-fvv.xml",
    fixture_process_evap_leaks => "process-evap-leaks.xml",
    fixture_process_evap_permeation => "process-evap-permeation.xml",
    fixture_process_pm_exhaust => "process-pm-exhaust.xml",
    fixture_process_refueling => "process-refueling.xml",
    fixture_process_tirewear => "process-tirewear.xml",
    fixture_scale_county => "scale-county.xml",
    fixture_scale_project => "scale-project.xml",
    fixture_scale_rates => "scale-rates.xml",
}

#[test]
fn every_fixture_is_covered_by_a_per_fixture_test() {
    // Defensive: the per-fixture macro is hand-maintained. If a new fixture
    // appears under characterization/fixtures, this test fails so we
    // remember to add it.
    let mut found = 0usize;
    for entry in std::fs::read_dir(fixtures_dir()).expect("read fixtures dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("xml") {
            found += 1;
        }
    }
    assert_eq!(
        found, 33,
        "expected 33 XML fixtures; bump the per_fixture_round_trip! macro when adding one"
    );
}

// --- Java RunSpecTest port ----------------------------------------------
//
// The canonical MOVES Java unit test `RunSpecTest.testSampleValues` reads
// `sample-runspec.xml` and asserts the parsed structure against the values
// `RunSpec` instances are built with for the rest of the test suite. We
// port those assertions one-to-one so any drift between the Rust model
// and the Java contract trips a test.

#[test]
fn java_runspec_test_sample_values() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let spec = from_xml_str(&xml).expect("parse sample-runspec.xml");

    // Description is empty in the sample fixture (just the placeholder
    // `<description></description>` tag).
    assert_eq!(spec.description, None);

    // MACROSCALE = inventory output (the default mode for sample-runspec.xml).
    assert_eq!(spec.scale, ModelScale::Macro);

    // Geographic selection: Washtenaw County, MI (FIPS 26161).
    assert_eq!(spec.geographic_selections.len(), 1);
    let g = &spec.geographic_selections[0];
    assert_eq!(g.kind, GeoKind::County);
    assert_eq!(g.key, 26161);
    assert_eq!(g.description, "MICHIGAN - Washtenaw County");

    // Time span: hour 6 of June 2001 (the smoke-test slice).
    assert_eq!(spec.timespan.years, vec![2001]);
    assert_eq!(spec.timespan.months, vec![6]);
    assert_eq!(spec.timespan.begin_hour, Some(6));
    assert_eq!(spec.timespan.end_hour, Some(6));

    // Single vehicle selection: gasoline passenger cars.
    assert_eq!(spec.onroad_vehicle_selections.len(), 1);
    let v = &spec.onroad_vehicle_selections[0];
    assert_eq!(v.fuel_type_id, 1);
    assert_eq!(v.fuel_type_name, "Gasoline");
    assert_eq!(v.source_type_id, 21);
    assert_eq!(v.source_type_name, "Passenger Car");

    // Single road type: urban restricted access.
    assert_eq!(spec.road_types.len(), 1);
    assert_eq!(spec.road_types[0].road_type_id, 4);
    assert_eq!(spec.road_types[0].road_type_name, "Urban Restricted Access");

    // Pollutant-process associations: three energy pollutants × three
    // processes + Well-to-Pump on Total Energy = 10 rows.
    assert_eq!(spec.pollutant_process_associations.len(), 10);

    // Uncertainty mode is off.
    assert!(!spec.uncertainty.enabled);

    // Output detail: county-level rollup.
    assert_eq!(
        spec.geographic_output_detail,
        GeographicOutputDetail::County
    );

    // A representative subset of the output-breakdown flags. The full
    // shape is exercised by the round-trip tests; we sample the values
    // that the Java test asserts explicitly.
    let b = &spec.output_breakdown;
    assert!(b.model_year);
    assert!(b.fuel_type);
    assert!(b.emission_process);
    assert!(!b.source_use_type);
    assert!(b.onroad_scc);

    // Output database is JUnitTestOutput (the Java test's well-known sentinel).
    assert_eq!(spec.output_database.database, "JUnitTestOutput");
    assert_eq!(spec.output_timestep, OutputTimestep::Hour);
    assert!(!spec.output_vmt_data);
    assert_eq!(spec.pm_size, 0);

    // Output factors: seconds, miles, grams, million BTU; all enabled.
    let f = &spec.output_factors;
    assert!(f.time.enabled);
    assert!(f.distance.enabled);
    assert!(f.mass.enabled);
    assert_eq!(f.time.units, TimeUnit::Seconds);
    assert_eq!(f.distance.units, DistanceUnit::Miles);
    assert_eq!(f.mass.units, MassUnit::Grams);
    assert_eq!(f.mass.energy_units, EnergyUnit::MillionBtu);

    // No internal control strategies and no database selections in the
    // fixture (those are open-ended placeholder containers).
    assert!(spec.internal_control_strategies.is_empty());
    assert!(spec.database_selections.is_empty());
}

#[test]
fn java_runspec_test_build_in_code_round_trip() {
    // Mirrors the build-and-round-trip pattern from the Java `RunSpecTest`:
    // assemble a RunSpec programmatically, serialize to XML, reparse, and
    // verify the structure survives. Combined with the byte-idempotence
    // check this proves the model ↔ XML mapping is lossless for the fields
    // the Java test exercises.
    let spec = RunSpec {
        description: Some("Test Description".to_string()),
        scale: ModelScale::Macro,
        geographic_selections: vec![
            GeographicSelection {
                kind: GeoKind::County,
                key: 39035,
                description: "Cuyahoga County".to_string(),
            },
            GeographicSelection {
                kind: GeoKind::County,
                key: 26161,
                description: "Washtenaw County".to_string(),
            },
        ],
        timespan: moves_runspec::Timespan {
            years: vec![2001],
            months: vec![6],
            days: (1..=7).collect(),
            begin_hour: Some(1),
            end_hour: Some(24),
            aggregate_by: None,
        },
        onroad_vehicle_selections: vec![OnroadVehicleSelection {
            fuel_type_id: 100,
            fuel_type_name: "Fuel BBB".to_string(),
            source_type_id: 200,
            source_type_name: "SourceUseType YYY".to_string(),
        }],
        road_types: vec![RoadType {
            road_type_id: 4,
            road_type_name: "Urban Restricted Access".to_string(),
            model_combination: None,
        }],
        pollutant_process_associations: vec![PollutantProcessAssociation {
            pollutant_id: 91,
            pollutant_name: "Total Energy Consumption".to_string(),
            process_id: 1,
            process_name: "Running Exhaust".to_string(),
        }],
        geographic_output_detail: GeographicOutputDetail::State,
        output_database: DatabaseRef {
            server: "localhost".to_string(),
            database: "JUnitTestOUTPUT".to_string(),
            description: String::new(),
        },
        output_timestep: OutputTimestep::Month,
        output_vmt_data: true,
        pm_size: 5,
        output_factors: moves_runspec::OutputFactors {
            time: moves_runspec::TimeFactor {
                enabled: true,
                units: TimeUnit::Seconds,
            },
            distance: moves_runspec::DistanceFactor::default(),
            mass: moves_runspec::MassFactor {
                enabled: true,
                units: MassUnit::Kilograms,
                ..moves_runspec::MassFactor::default()
            },
        },
        ..RunSpec::default()
    };

    // First serialization.
    let xml1 = to_xml_string(&spec).expect("serialize");
    let reparsed = from_xml_str(&xml1).expect("reparse");
    assert_eq!(spec, reparsed, "build-in-code RunSpec must survive XML");

    // Byte-identical idempotence on the build-in-code spec too.
    let xml2 = to_xml_string(&reparsed).expect("re-serialize");
    assert_eq!(
        xml1, xml2,
        "the serializer is idempotent on a hand-built RunSpec"
    );

    // The fields the Java test calls out explicitly.
    assert_eq!(reparsed.description.as_deref(), Some("Test Description"));
    assert_eq!(reparsed.output_database.database, "JUnitTestOUTPUT");
    assert_eq!(reparsed.output_database.server, "localhost");
    assert_eq!(reparsed.output_timestep, OutputTimestep::Month);
    assert_eq!(reparsed.pm_size, 5);
    let keys: std::collections::BTreeSet<u32> = reparsed
        .geographic_selections
        .iter()
        .map(|g| g.key)
        .collect();
    assert_eq!(keys, [26161, 39035].into_iter().collect());
}

#[test]
fn java_runspec_xml_test_version_preserved() {
    // Mirrors `RunSpecXMLTest`'s version-attribute check: the optional
    // version attribute on `<runspec>` survives parse → serialize → parse.
    let xml = read(&fixture(
        "characterization/fixtures/nr-agriculture-state.xml",
    ));
    let spec = from_xml_str(&xml).expect("parse");
    assert_eq!(spec.version.as_deref(), Some("MOVES5.0.1"));

    let out = to_xml_string(&spec).expect("serialize");
    assert!(
        out.starts_with("<runspec version=\"MOVES5.0.1\">\n"),
        "version attribute must round-trip onto the root element: {}",
        &out[..out.find('\n').unwrap_or(out.len())]
    );

    // And the absence of a version attribute round-trips too.
    let sample = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let spec = from_xml_str(&sample).expect("parse");
    assert_eq!(spec.version, None);
    let out = to_xml_string(&spec).expect("serialize");
    assert!(
        out.starts_with("<runspec>\n"),
        "missing version attribute must stay missing: {}",
        &out[..out.find('\n').unwrap_or(out.len())]
    );
}

#[test]
fn java_runspec_xml_test_nonroad_output_flags() {
    // NONROAD fixtures carry the six `<outputsho>` / `<outputsh>` / ... flags
    // that ONROAD fixtures omit. Verify they survive XML → model → XML.
    let xml = read(&fixture(
        "characterization/fixtures/nr-agriculture-state.xml",
    ));
    let spec = from_xml_str(&xml).expect("parse");
    assert_eq!(spec.output_nonroad.sho, Some(false));
    assert_eq!(spec.output_nonroad.sh, Some(false));
    assert_eq!(spec.output_nonroad.shp, Some(false));
    assert_eq!(spec.output_nonroad.shidling, Some(false));
    assert_eq!(spec.output_nonroad.starts, Some(false));
    assert_eq!(spec.output_nonroad.population, Some(false));

    let out = to_xml_string(&spec).expect("serialize");
    let reparsed = from_xml_str(&out).expect("reparse");
    assert_eq!(spec, reparsed);
    assert!(out.contains("<outputsho value=\"false\"/>"));
    assert!(out.contains("<outputpopulation value=\"false\"/>"));
}

#[test]
fn java_runspec_xml_test_models_and_domain() {
    // ONROAD/NONROAD model selectors and the modeldomain element round-trip.
    let xml = read(&fixture(
        "characterization/fixtures/nr-agriculture-state.xml",
    ));
    let spec = from_xml_str(&xml).expect("parse");
    assert_eq!(spec.models, vec![Model::Nonroad]);
    assert!(spec.domain.is_some());

    let out = to_xml_string(&spec).expect("serialize");
    assert!(out.contains("<model value=\"NONROAD\"/>"));
    assert!(out.contains("<modeldomain value=\"DEFAULT\"/>"));
}

#[test]
fn java_runspec_xml_test_aggregate_by() {
    // `<aggregateBy>` is one of the optional timespan children. It appears
    // in only a couple of fixtures; round-tripping is the test.
    let xml = read(&fixture("characterization/fixtures/expand-month.xml"));
    let spec = from_xml_str(&xml).expect("parse");
    assert_eq!(spec.timespan.aggregate_by.as_deref(), Some("Month"));

    let out = to_xml_string(&spec).expect("serialize");
    assert!(out.contains("<aggregateBy key=\"Month\"/>"));
    let reparsed = from_xml_str(&out).expect("reparse");
    assert_eq!(spec.timespan, reparsed.timespan);
}

#[test]
fn java_runspec_xml_test_serializer_is_byte_idempotent() {
    // The Java `RunSpecXMLTest.testRoundTrip` checks that emitting a
    // parsed RunSpec produces the same bytes the parser would re-read.
    // We test the stronger property: across all 33 fixtures, after one
    // model round-trip the serializer is byte-identical on every
    // subsequent re-serialization.
    let mut failures = vec![];
    for entry in std::fs::read_dir(fixtures_dir()).expect("read fixtures dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("xml") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let xml = read(&path);
        let spec = match from_xml_str(&xml) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{name}: parse failed: {e}"));
                continue;
            }
        };
        let out1 = to_xml_string(&spec).expect("serialize");
        let out2 = to_xml_string(&from_xml_str(&out1).expect("reparse")).expect("re-serialize");
        if out1 != out2 {
            failures.push(format!("{name}: not byte-idempotent"));
        }
    }
    assert!(
        failures.is_empty(),
        "byte-idempotence failed on:\n{}",
        failures.join("\n")
    );
}

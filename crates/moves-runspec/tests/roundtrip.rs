//! Round-trip tests for `moves-runspec`.
//!
//! Replaces the Java `RunSpecXMLTest` (which only had one method): for every
//! characterization fixture, parse → serialize → parse must produce an
//! equal `RunSpec`, and serialize → parse → serialize must be byte-identical.
//! These two conditions together imply the parser and serializer are
//! lossless and the canonical serialization form is stable.
//!
//! Each fixture is exercised as a separate test case so failures point at
//! the specific RunSpec that broke.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;

use moves_runspec::types::*;
use moves_runspec::{parse_runspec, serialize_runspec};

fn fixtures_dir() -> PathBuf {
    // Walk up from `crates/moves-runspec` to the workspace root, then into
    // `characterization/fixtures`. This keeps the tests usable whether
    // run from the workspace root or from inside the crate.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates
    p.pop(); // workspace root
    p.push("characterization");
    p.push("fixtures");
    p
}

fn roundtrip_fixture(name: &str) {
    let path = fixtures_dir().join(name);
    let bytes = std::fs::read(&path).unwrap_or_else(|e| {
        panic!("read fixture {}: {e}", path.display());
    });
    let parsed = parse_runspec(&bytes).expect("parse");
    let serialized = serialize_runspec(&parsed).expect("serialize");
    let reparsed = parse_runspec(&serialized).expect("reparse");
    assert_eq!(parsed, reparsed, "round-trip mismatch on {name}");
    let reserialized = serialize_runspec(&reparsed).expect("re-serialize");
    assert_eq!(
        serialized, reserialized,
        "serialization not idempotent on {name}"
    );
}

macro_rules! roundtrip_tests {
    ($($name:ident => $file:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                roundtrip_fixture($file);
            }
        )*
    };
}

roundtrip_tests! {
    sample_runspec => "sample-runspec.xml",
    expand_counties => "expand-counties.xml",
    expand_criteria => "expand-criteria.xml",
    expand_day => "expand-day.xml",
    expand_fueltype_diesel => "expand-fueltype-diesel.xml",
    expand_month => "expand-month.xml",
    expand_sourcetype => "expand-sourcetype.xml",
    chain_tog_speciation => "chain-tog-speciation.xml",
    nr_agriculture_state => "nr-agriculture-state.xml",
    nr_airport_support_county => "nr-airport-support-county.xml",
    nr_commercial_nation => "nr-commercial-nation.xml",
    nr_construction_state => "nr-construction-state.xml",
    nr_industrial_county => "nr-industrial-county.xml",
    nr_lawn_garden_county => "nr-lawn-garden-county.xml",
    nr_logging_county => "nr-logging-county.xml",
    nr_pleasure_craft_state => "nr-pleasure-craft-state.xml",
    nr_railroad_support_nation => "nr-railroad-support-nation.xml",
    nr_recreational_county => "nr-recreational-county.xml",
    process_airtoxics => "process-airtoxics.xml",
    process_apu => "process-apu.xml",
    process_brakewear => "process-brakewear.xml",
    process_crankcase_extidle => "process-crankcase-extidle.xml",
    process_crankcase_running => "process-crankcase-running.xml",
    process_crankcase_start => "process-crankcase-start.xml",
    process_evap_fvv => "process-evap-fvv.xml",
    process_evap_leaks => "process-evap-leaks.xml",
    process_evap_permeation => "process-evap-permeation.xml",
    process_pm_exhaust => "process-pm-exhaust.xml",
    process_refueling => "process-refueling.xml",
    process_tirewear => "process-tirewear.xml",
    scale_county => "scale-county.xml",
    scale_project => "scale-project.xml",
    scale_rates => "scale-rates.xml",
}

/// Verifies the sample fixture's parsed contents match the canonical Java
/// `RunSpecTest.setSampleValues` shape (county/year/month/fueltype/etc.).
/// Ports the only `RunSpecTest.testSampleValues` smoke test to Rust.
#[test]
fn sample_runspec_fields_match_canonical_java_test() {
    let bytes = std::fs::read(fixtures_dir().join("sample-runspec.xml")).unwrap();
    let spec = parse_runspec(&bytes).unwrap();

    // Description is empty in the fixture.
    assert_eq!(spec.description, "");
    // MACROSCALE / inventory mode is the default for sample-runspec.xml.
    assert_eq!(spec.scale, Some(ModelScale::Macroscale));

    let want_geo: BTreeSet<GeographicSelection> = [GeographicSelection {
        type_: GeographicSelectionType::County,
        database_key: 26161,
        text_description: "MICHIGAN - Washtenaw County".to_string(),
    }]
    .into_iter()
    .collect();
    assert_eq!(spec.geographic_selections, want_geo);

    assert!(spec.time_span.years.contains(&2001));
    assert!(spec.time_span.months.contains(&6));
    assert_eq!(spec.time_span.begin_hour_id, 6);
    assert_eq!(spec.time_span.end_hour_id, 6);

    let want_onroad: BTreeSet<OnRoadVehicleSelection> = [OnRoadVehicleSelection {
        fuel_type_id: 1,
        fuel_type_desc: "Gasoline".to_string(),
        source_type_id: 21,
        source_type_name: "Passenger Car".to_string(),
    }]
    .into_iter()
    .collect();
    assert_eq!(spec.onroad_vehicle_selections, want_onroad);

    let want_road: BTreeSet<RoadType> = [RoadType {
        road_type_id: 4,
        road_type_name: "Urban Restricted Access".to_string(),
        model_combination: ModelCombination::M1,
    }]
    .into_iter()
    .collect();
    assert_eq!(spec.road_types, want_road);

    // Three energy pollutants × three processes + Well-to-Pump on pollutant 91.
    assert_eq!(spec.pollutant_process_associations.len(), 10);

    assert!(!spec.uncertainty_parameters.uncertainty_mode_enabled);
    assert_eq!(
        spec.geographic_output_detail,
        Some(GeographicOutputDetailLevel::County)
    );

    let oeb = &spec.output_emissions_breakdown_selection;
    assert!(oeb.model_year);
    assert!(oeb.fuel_type);
    assert!(oeb.emission_process);
    assert!(!oeb.source_use_type);
    assert!(oeb.onroad_scc);

    assert_eq!(
        spec.output_database.database_name,
        "JUnitTestOutput".to_string()
    );
    assert_eq!(spec.output_time_step, Some(OutputTimeStep::Hour));
    assert!(!spec.output_vmt_data);
    assert_eq!(spec.pm_size, 0);

    let factors = &spec.output_factors;
    assert!(factors.time_factors_selected);
    assert!(factors.distance_factors_selected);
    assert!(factors.mass_factors_selected);
    assert_eq!(
        factors.time_measurement_system,
        Some(TimeMeasurementSystem::Seconds)
    );
    assert_eq!(
        factors.distance_measurement_system,
        Some(DistanceMeasurementSystem::Miles)
    );
    assert_eq!(
        factors.mass_measurement_system,
        Some(MassMeasurementSystem::Grams)
    );
    assert_eq!(
        factors.energy_measurement_system,
        Some(EnergyMeasurementSystem::MillionBtu)
    );

    // No internal control strategies in the fixture.
    let empty: BTreeMap<String, Vec<InternalControlStrategy>> = BTreeMap::new();
    assert_eq!(spec.internal_control_strategies, empty);
}

/// Ports `RunSpecTest.testSampleValues` to Rust: assembling a RunSpec in code,
/// round-tripping it through XML, must yield an equal RunSpec.
#[test]
fn build_in_code_round_trip() {
    use std::collections::BTreeSet;
    let mut spec = RunSpec {
        description: "Test Description".to_string(),
        scale: Some(ModelScale::Macroscale),
        ..RunSpec::default()
    };

    spec.geographic_selections.insert(GeographicSelection {
        type_: GeographicSelectionType::County,
        database_key: 39035,
        text_description: "Cuyahoga County".to_string(),
    });
    spec.geographic_selections.insert(GeographicSelection {
        type_: GeographicSelectionType::County,
        database_key: 26161,
        text_description: "Washtenaw County".to_string(),
    });

    spec.time_span.years.insert(2001);
    spec.time_span.months.insert(6);
    for d in 1..=7 {
        spec.time_span.days.insert(d);
    }
    spec.time_span.begin_hour_id = 1;
    spec.time_span.end_hour_id = 24;

    spec.onroad_vehicle_selections
        .insert(OnRoadVehicleSelection {
            fuel_type_id: 100,
            fuel_type_desc: "Fuel BBB".to_string(),
            source_type_id: 200,
            source_type_name: "SourceUseType YYY".to_string(),
        });
    spec.road_types.insert(RoadType {
        road_type_id: 4,
        road_type_name: "Urban Restricted Access".to_string(),
        model_combination: ModelCombination::M1,
    });
    spec.pollutant_process_associations
        .insert(PollutantProcessAssociation {
            pollutant_key: 91,
            pollutant_name: "Total Energy Consumption".to_string(),
            process_key: 1,
            process_name: "Running Exhaust".to_string(),
        });

    spec.geographic_output_detail = Some(GeographicOutputDetailLevel::State);
    spec.output_emissions_breakdown_selection.emission_process = false;
    spec.output_database.database_name = "JUnitTestOUTPUT".to_string();
    spec.output_database.server_name = "localhost".to_string();
    spec.output_time_step = Some(OutputTimeStep::Month);
    spec.output_vmt_data = true;
    spec.pm_size = 5;
    spec.output_factors.time_factors_selected = true;
    spec.output_factors.mass_factors_selected = true;
    spec.output_factors.time_measurement_system = Some(TimeMeasurementSystem::Seconds);
    spec.output_factors.mass_measurement_system = Some(MassMeasurementSystem::Kilograms);

    let bytes = serialize_runspec(&spec).unwrap();
    let reparsed = parse_runspec(&bytes).unwrap();

    // Re-serializing the re-parsed spec must be byte-identical to the first.
    let bytes2 = serialize_runspec(&reparsed).unwrap();
    assert_eq!(bytes, bytes2);

    // Geographic selections preserved.
    let got: BTreeSet<_> = reparsed
        .geographic_selections
        .iter()
        .map(|g| g.database_key)
        .collect();
    assert_eq!(got, BTreeSet::from([39035, 26161]));

    // Description preserved.
    assert_eq!(reparsed.description, "Test Description");
    assert_eq!(reparsed.output_database.database_name, "JUnitTestOUTPUT");
    assert_eq!(reparsed.output_time_step, Some(OutputTimeStep::Month));
    assert_eq!(reparsed.pm_size, 5);
}

/// Smoke test that the version attribute is preserved.
#[test]
fn version_is_round_tripped() {
    let bytes = std::fs::read(fixtures_dir().join("nr-agriculture-state.xml")).unwrap();
    let spec = parse_runspec(&bytes).unwrap();
    assert_eq!(spec.version, "MOVES5.0.1");
}

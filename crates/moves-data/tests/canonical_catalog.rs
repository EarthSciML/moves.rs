//! Cross-module consistency checks for the canonical catalog tables.
//!
//! The static tables in `pollutant.rs`, `process.rs`, and
//! `pollutant_process.rs` were generated from the same execution-chain
//! snapshot. These tests catch any future hand-edit that pulls one out
//! of sync with the others — every association must reference pollutants
//! and processes that exist in their respective tables, and the composite
//! `polProcessID` must round-trip without loss.

use moves_data::{
    EmissionProcess, PolProcessId, Pollutant, PollutantProcessAssociation, RoadType, SourceType,
};

#[test]
fn every_canonical_association_references_a_canonical_pollutant() {
    for assoc in PollutantProcessAssociation::all() {
        assert!(
            Pollutant::find_by_id(assoc.pollutant_id).is_some(),
            "association {assoc:?} references unknown pollutant"
        );
    }
}

#[test]
fn every_canonical_association_references_a_canonical_process() {
    for assoc in PollutantProcessAssociation::all() {
        assert!(
            EmissionProcess::find_by_id(assoc.process_id).is_some(),
            "association {assoc:?} references unknown process"
        );
    }
}

#[test]
fn polproc_id_round_trips_through_compose_and_decompose() {
    for assoc in PollutantProcessAssociation::all() {
        let id = assoc.polproc_id();
        assert_eq!(id.pollutant_id(), assoc.pollutant_id);
        assert_eq!(id.process_id(), assoc.process_id);
        // And `new` agrees with `polproc_id`.
        assert_eq!(PolProcessId::new(assoc.pollutant_id, assoc.process_id), id);
    }
}

#[test]
fn road_and_source_type_catalogs_are_non_empty() {
    // Sanity floors so a future generator that empties these tables fails
    // loudly.
    assert!(RoadType::all().count() >= 5);
    assert!(SourceType::all().count() >= 13);
}

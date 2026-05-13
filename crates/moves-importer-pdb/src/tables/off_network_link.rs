//! `offNetworkLink` table importer — port of `OffNetworkLinkImporter.java`.
//!
//! Per `OffNetworkLinkImporter.getProjectDataStatus` (lines 154-169):
//! the table is only required when the RunSpec selects road type 1
//! (off-network); when selected, every selected source type must have
//! a row. [`validate_against_runspec`] enforces that.

use std::collections::BTreeSet;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::csv_reader::{read_csv, ImportReport};
use crate::error::Result;
use crate::filter::RunSpecFilter;
use crate::schema::OFF_NETWORK_LINK;
use crate::validate;

/// Off-network road type identifier (Java
/// `OffNetworkLinkImporter.offnetworkRoadTypeID`).
pub const OFFNETWORK_ROAD_TYPE: i64 = 1;

/// Read a CSV `offNetworkLink` file.
pub fn read(path: &Path, runspec: &RunSpecFilter) -> Result<ImportReport> {
    read_csv(path, &OFF_NETWORK_LINK, runspec)
}

/// Source-type coverage check, gated on whether the RunSpec selects
/// road type 1. `selected_road_types` is the runspec's road-type set;
/// `expected_source_types` is its source-type set.
pub fn validate_against_runspec(
    off_network_link: &RecordBatch,
    selected_road_types: &BTreeSet<i64>,
    expected_source_types: &BTreeSet<i64>,
) -> Result<()> {
    if !selected_road_types.contains(&OFFNETWORK_ROAD_TYPE) {
        return Ok(());
    }
    validate::validate_off_network_link(off_network_link, expected_source_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn round_trip_and_coverage() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "zoneID,sourceTypeID,vehiclePopulation,startFraction,extendedIdleFraction,parkedVehicleFraction
261610,21,1000,0.1,0.0,0.9
261610,32,500,0.05,0.2,0.75
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        assert_eq!(report.batch.num_rows(), 2);
        validate_against_runspec(
            &report.batch,
            &BTreeSet::from([1, 4]),
            &BTreeSet::from([21, 32]),
        )
        .unwrap();
    }

    #[test]
    fn coverage_skipped_when_offnetwork_not_selected() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "zoneID,sourceTypeID,vehiclePopulation,startFraction,extendedIdleFraction,parkedVehicleFraction
261610,21,1000,0.1,0.0,0.9
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        // Source type 32 is missing, but road type 1 isn't selected → no error.
        validate_against_runspec(
            &report.batch,
            &BTreeSet::from([4]),
            &BTreeSet::from([21, 32]),
        )
        .unwrap();
    }
}

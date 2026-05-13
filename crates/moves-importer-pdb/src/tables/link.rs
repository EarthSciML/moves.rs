//! `Link` table importer — port of `LinkImporter.java`.
//!
//! The Java importer's only RunSpec-time enforcement (beyond the
//! per-cell filters declared in the descriptor) is in
//! `LinkImporter.getProjectDataStatus`: every selected county, zone,
//! and road type must appear in the loaded Link table. The
//! [`validate_runspec_coverage`] helper exposes that check; per-cell
//! validation runs inside [`crate::csv_reader::read_csv`].

use std::collections::BTreeSet;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::csv_reader::{read_csv, ImportReport};
use crate::error::Result;
use crate::filter::RunSpecFilter;
use crate::schema::LINK;
use crate::validate;

/// Read a CSV `Link` file into an in-memory [`RecordBatch`] plus
/// warnings. The runspec is consulted only for membership filters
/// (county/zone/roadType); pass [`RunSpecFilter::default`] for
/// standalone use.
pub fn read(path: &Path, runspec: &RunSpecFilter) -> Result<ImportReport> {
    read_csv(path, &LINK, runspec)
}

/// Verify that the loaded Link table covers every county, zone, and
/// road type the RunSpec selected. Mirrors
/// `LinkImporter.getProjectDataStatus` (lines 155-183 of
/// `LinkImporter.java`).
pub fn validate_runspec_coverage(
    link: &RecordBatch,
    expected_counties: &BTreeSet<i64>,
    expected_zones: &BTreeSet<i64>,
    expected_road_types: &BTreeSet<i64>,
) -> Result<()> {
    validate::validate_link(link, expected_counties, expected_zones, expected_road_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn read_and_validate_round_trip() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
2,26161,261610,2,1.0,500,40,b,0
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        assert_eq!(report.batch.num_rows(), 2);
        validate_runspec_coverage(
            &report.batch,
            &BTreeSet::from([26161]),
            &BTreeSet::from([261610]),
            &BTreeSet::from([2, 4]),
        )
        .unwrap();
    }
}

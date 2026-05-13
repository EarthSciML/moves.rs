//! `linkSourceTypeHour` table importer — port of `LinkSourceTypeHourImporter.java`.
//!
//! The Java importer's `getProjectDataStatus` runs three SQL checks
//! after loading. [`validate_against_link`] re-implements all three
//! against an in-memory Arrow batch instead of MariaDB.

use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::csv_reader::{read_csv, ImportReport};
use crate::error::Result;
use crate::filter::RunSpecFilter;
use crate::schema::LINK_SOURCE_TYPE_HOUR;
use crate::validate;

/// Read a CSV `linkSourceTypeHour` file.
pub fn read(path: &Path, runspec: &RunSpecFilter) -> Result<ImportReport> {
    read_csv(path, &LINK_SOURCE_TYPE_HOUR, runspec)
}

/// Run the three Java-side invariants:
/// 1. `sum(sourceTypeHourFraction)` per linkID = 1.0 (4 dp)
/// 2. linkIDs with `roadTypeID == 1` (off-network) MUST NOT appear
/// 3. every link with `roadTypeID != 1` MUST appear
pub fn validate_against_link(
    link_source_type_hour: &RecordBatch,
    link: &RecordBatch,
) -> Result<()> {
    validate::validate_link_source_type_hour(link_source_type_hour, Some(link))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_reader::read_csv_from_reader;
    use crate::schema::LINK;
    use std::io::Cursor;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn full_round_trip_with_link_validation() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "linkID,sourceTypeID,sourceTypeHourFraction
1,21,0.4
1,32,0.6
2,21,1.0
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        assert_eq!(report.batch.num_rows(), 3);
        let link = read_csv_from_reader(
            Cursor::new(
                "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
2,26161,261610,2,1.0,500,45,b,0
99,26161,261610,1,0,0,0,offnet,
"
                    .as_bytes(),
            ),
            std::path::Path::new("(test)"),
            &LINK,
            &RunSpecFilter::default(),
        )
        .unwrap()
        .batch;
        validate_against_link(&report.batch, &link).unwrap();
    }
}

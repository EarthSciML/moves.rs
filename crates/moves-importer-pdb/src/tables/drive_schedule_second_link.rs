//! `driveScheduleSecondLink` table importer — port of `DriveScheduleSecondLinkImporter.java`.
//!
//! The Java importer has no SQL-level invariants (the source comments
//! explicitly note "no SQL error checks for DriveScheduleSecondLinkImporter"
//! at line 155). All validation is per-cell:
//!   - `linkID`, `secondID`, `speed` are NOT NULL
//!   - `secondID` must be ≥ 0 (FILTER_NON_NEGATIVE)
//!   - `speed` must be ≥ 0 (FILTER_NON_NEGATIVE)
//!
//! Both filters are no-ops in Java (FILTER_NON_NEGATIVE accepts any
//! finite f64 there); we apply the same semantic in [`crate::filter`].

use std::path::Path;

use crate::csv_reader::{read_csv, ImportReport};
use crate::error::Result;
use crate::filter::RunSpecFilter;
use crate::schema::DRIVE_SCHEDULE_SECOND_LINK;

/// Read a CSV `driveScheduleSecondLink` file.
pub fn read(path: &Path, runspec: &RunSpecFilter) -> Result<ImportReport> {
    read_csv(path, &DRIVE_SCHEDULE_SECOND_LINK, runspec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn round_trip() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "linkID,secondID,speed,grade
1,0,0,0
1,1,5.5,0.5
1,2,12.3,-0.3
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        assert_eq!(report.batch.num_rows(), 3);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn grade_is_nullable() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "linkID,secondID,speed,grade\n1,0,0,\n").unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        assert!(report.batch.column(3).is_null(0));
    }
}

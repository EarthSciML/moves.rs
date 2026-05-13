//! `OpModeDistribution` table importer — port of `LinkOpmodeDistributionImporter.java`.
//!
//! Per Java's `getProjectDataStatus` (lines 254-300):
//! - The table is required only when an off-network link is present
//!   (`Link` table contains a row with `roadTypeID == 1`).
//! - When required: every selected source type and hour-day must
//!   appear in OpModeDistribution.
//!
//! Java further filters the *template* (the empty file the GUI hands
//! to users) to drop hotelling/auxiliary op modes via
//! `MyBasicDataHandler.shouldWriteTemplateRow`. We don't generate
//! templates here — that's a GUI/CLI concern outside the importer's
//! load path — so the per-row template filter doesn't carry across.
//! When a CLI template generator lands, lift the predicate from
//! `LinkOpmodeDistributionImporter.java` lines 174-220.

use std::collections::BTreeSet;
use std::path::Path;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;

use crate::csv_reader::{read_csv, ImportReport};
use crate::error::Result;
use crate::filter::RunSpecFilter;
use crate::schema::OP_MODE_DISTRIBUTION;
use crate::tables::off_network_link::OFFNETWORK_ROAD_TYPE;
use crate::validate;

/// Read a CSV `OpModeDistribution` file.
pub fn read(path: &Path, runspec: &RunSpecFilter) -> Result<ImportReport> {
    read_csv(path, &OP_MODE_DISTRIBUTION, runspec)
}

/// Java's coverage check, gated on the presence of any off-network
/// link in the loaded `Link` table.
pub fn validate_against_runspec(
    op_mode_distribution: &RecordBatch,
    link: &RecordBatch,
    expected_source_types: &BTreeSet<i64>,
    expected_hour_days: &BTreeSet<i64>,
) -> Result<()> {
    if !any_off_network_link(link)? {
        return Ok(());
    }
    validate::validate_op_mode_distribution(
        op_mode_distribution,
        expected_source_types,
        expected_hour_days,
    )
}

fn any_off_network_link(link: &RecordBatch) -> Result<bool> {
    let idx =
        link.schema()
            .index_of("roadTypeID")
            .map_err(|_| crate::error::Error::Validation {
                table: "Link".into(),
                message: "column 'roadTypeID' not found".into(),
            })?;
    let arr = link
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::error::Error::Validation {
            table: "Link".into(),
            message: "column 'roadTypeID' is not Int64".into(),
        })?;
    for i in 0..arr.len() {
        if !arr.is_null(i) && arr.value(i) == OFFNETWORK_ROAD_TYPE {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_reader::read_csv_from_reader;
    use crate::schema::LINK;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn link(csv: &str) -> RecordBatch {
        read_csv_from_reader(
            Cursor::new(csv.as_bytes()),
            std::path::Path::new("(test)"),
            &LINK,
            &RunSpecFilter::default(),
        )
        .unwrap()
        .batch
    }

    #[test]
    fn coverage_skipped_when_no_off_network_link() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "sourceTypeID,hourDayID,linkID,polProcessID,opModeID,opModeFraction
21,55,1,1101,0,1.0
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        let onnet = link(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,1,1,55,a,
",
        );
        // hourDayID set asks for 75 — but no off-network link, so check skips.
        validate_against_runspec(
            &report.batch,
            &onnet,
            &BTreeSet::from([21]),
            &BTreeSet::from([55, 75]),
        )
        .unwrap();
    }

    #[test]
    fn coverage_enforced_when_off_network_link_present() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            "sourceTypeID,hourDayID,linkID,polProcessID,opModeID,opModeFraction
21,55,1,1101,0,1.0
"
        )
        .unwrap();
        let report = read(tmp.path(), &RunSpecFilter::default()).unwrap();
        let with_off = link(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,1,1,55,a,
99,26161,261610,1,0,0,0,offnet,
",
        );
        let err = validate_against_runspec(
            &report.batch,
            &with_off,
            &BTreeSet::from([21]),
            &BTreeSet::from([55, 75]),
        )
        .unwrap_err();
        match err {
            crate::error::Error::Validation { message, .. } => {
                assert!(message.contains("75"), "got: {message}");
            }
            other => panic!("wanted Validation, got {other:?}"),
        }
    }
}

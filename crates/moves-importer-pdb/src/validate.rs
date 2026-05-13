//! Cross-row validation invariants.
//!
//! Mirrors the per-importer `getProjectDataStatus` SQL checks that
//! Java runs *after* loading data into MariaDB:
//!
//! * `LinkSourceTypeHourImporter.getProjectDataStatus`
//!   (lines 149-237 of `LinkSourceTypeHourImporter.java`):
//!     - `sum(sourceTypeHourFraction) by linkID = 1.0` (rounded 4 dp)
//!     - linkID with `roadTypeID == 1` (off-network) MUST NOT appear
//!     - every link with `roadTypeID != 1` MUST appear
//! * `LinkOpmodeDistributionImporter.getProjectDataStatus`
//!   (lines 254-300 of `LinkOpmodeDistributionImporter.java`):
//!     - if any off-network link present (`roadTypeID == 1`),
//!       OpModeDistribution must cover every selected source type
//!       and hour-day. (We expose a helper that callers can use to
//!       check this against their RunSpec selections.)
//! * `OffNetworkLinkImporter.getProjectDataStatus`
//!   (lines 154-169 of `OffNetworkLinkImporter.java`):
//!     - if `roadTypeID == 1` is selected in the RunSpec, the table
//!       MUST cover every selected source type.
//! * `LinkImporter.getProjectDataStatus`
//!   (lines 155-183 of `LinkImporter.java`): runspec-level coverage
//!   for zones, counties, road types — we expose those as helpers.
//!
//! These invariants take Arrow [`RecordBatch`] rather than a SQL
//! connection because there is no MariaDB in the Rust port — the data
//! lives in Arrow throughout the importer.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};

/// Tolerance used by Java's `ROUND(sourceTypeHourFractionTotal, 4) <> 1.0000`
/// check (line 187 of `LinkSourceTypeHourImporter.java`).
const FRACTION_SUM_TOLERANCE: f64 = 5e-5;

/// `linkSourceTypeHour` invariants. Operates on a [`RecordBatch`] with
/// the schema of [`crate::schema::LINK_SOURCE_TYPE_HOUR`] and a
/// reference [`RecordBatch`] for `Link` (schema of
/// [`crate::schema::LINK`]).
///
/// `link` may be `None` when the caller hasn't loaded the Link table
/// yet — in that case we skip the off-network and missing-link checks
/// (Java's `getProjectDataStatus` early-returns `OK` if `link` is
/// empty too) and only enforce the fraction-sum invariant.
pub fn validate_link_source_type_hour(
    link_source_type_hour: &RecordBatch,
    link: Option<&RecordBatch>,
) -> Result<()> {
    // ----- fraction-sum invariant -----
    let link_id = column_i64(link_source_type_hour, "linkID")?;
    let frac = column_f64(link_source_type_hour, "sourceTypeHourFraction")?;
    let mut sums: BTreeMap<i64, f64> = BTreeMap::new();
    for i in 0..link_source_type_hour.num_rows() {
        if link_id.is_null(i) || frac.is_null(i) {
            continue;
        }
        *sums.entry(link_id.value(i)).or_insert(0.0) += frac.value(i);
    }
    for (lid, total) in sums {
        if (total - 1.0).abs() > FRACTION_SUM_TOLERANCE {
            return Err(Error::Validation {
                table: "linkSourceTypeHour".into(),
                message: format!("sourceTypeHourFraction sums to {total} on linkID {lid}",),
            });
        }
    }

    // The off-network and missing-link checks need the Link table.
    let Some(link) = link else { return Ok(()) };
    let link_link_id = column_i64(link, "linkID")?;
    let link_road_type = column_i64(link, "roadTypeID")?;

    // Map linkID -> roadTypeID for the Link table.
    let mut link_road_types: BTreeMap<i64, i64> = BTreeMap::new();
    for i in 0..link.num_rows() {
        if link_link_id.is_null(i) || link_road_type.is_null(i) {
            continue;
        }
        link_road_types.insert(link_link_id.value(i), link_road_type.value(i));
    }

    // ----- off-network link should NOT be in linkSourceTypeHour -----
    let mut seen_lsth_links: BTreeSet<i64> = BTreeSet::new();
    for i in 0..link_source_type_hour.num_rows() {
        if link_id.is_null(i) {
            continue;
        }
        let lid = link_id.value(i);
        seen_lsth_links.insert(lid);
        if link_road_types.get(&lid) == Some(&1) {
            return Err(Error::Validation {
                table: "linkSourceTypeHour".into(),
                message: format!(
                    "linkID {lid} is the off-network link and should not be included in this table.",
                ),
            });
        }
    }

    // ----- every non-off-network link MUST appear in linkSourceTypeHour -----
    for (lid, road_type) in &link_road_types {
        if *road_type == 1 {
            continue;
        }
        if !seen_lsth_links.contains(lid) {
            return Err(Error::Validation {
                table: "linkSourceTypeHour".into(),
                message: format!("linkID {lid} is missing."),
            });
        }
    }

    Ok(())
}

/// `Link` invariants: every selected county, zone, and road type in
/// the RunSpec must have coverage in the Link table. Mirrors the
/// `manager.tableHasZones`/`tableHasCounties`/`tableHasRoadTypes`
/// calls in `LinkImporter.getProjectDataStatus`.
///
/// `expected_*` are the runspec-level allow-lists. Empty allow-list
/// → skip the check (Java's `getFilterValuesSet` returns the
/// configured set; empty means "no restriction" for the purpose of
/// these invariants since the data set is what defines coverage).
pub fn validate_link(
    link: &RecordBatch,
    expected_counties: &BTreeSet<i64>,
    expected_zones: &BTreeSet<i64>,
    expected_road_types: &BTreeSet<i64>,
) -> Result<()> {
    let county_id = column_i64(link, "countyID")?;
    let zone_id = column_i64(link, "zoneID")?;
    let road_type = column_i64(link, "roadTypeID")?;

    let mut seen_counties = BTreeSet::new();
    let mut seen_zones = BTreeSet::new();
    let mut seen_road_types = BTreeSet::new();
    for i in 0..link.num_rows() {
        if !county_id.is_null(i) {
            seen_counties.insert(county_id.value(i));
        }
        if !zone_id.is_null(i) {
            seen_zones.insert(zone_id.value(i));
        }
        if !road_type.is_null(i) {
            seen_road_types.insert(road_type.value(i));
        }
    }

    coverage_check("Link", "countyID", expected_counties, &seen_counties)?;
    coverage_check("Link", "zoneID", expected_zones, &seen_zones)?;
    coverage_check("Link", "roadTypeID", expected_road_types, &seen_road_types)?;
    Ok(())
}

/// `offNetworkLink` invariant. Java only enforces this when the
/// RunSpec has road type 1 selected; callers gate the call on that
/// condition.
///
/// `expected_source_types` mirrors `manager.tableHasSourceTypes` from
/// `OffNetworkLinkImporter.getProjectDataStatus` (line 162).
pub fn validate_off_network_link(
    off_network_link: &RecordBatch,
    expected_source_types: &BTreeSet<i64>,
) -> Result<()> {
    let source_type = column_i64(off_network_link, "sourceTypeID")?;
    let mut seen = BTreeSet::new();
    for i in 0..off_network_link.num_rows() {
        if !source_type.is_null(i) {
            seen.insert(source_type.value(i));
        }
    }
    coverage_check(
        "offNetworkLink",
        "sourceTypeID",
        expected_source_types,
        &seen,
    )
}

/// `OpModeDistribution` invariant: when an off-network link is
/// present in the RunSpec, the table must cover every selected source
/// type and hour-day. Mirrors `LinkOpmodeDistributionImporter.getProjectDataStatus`
/// (lines 286-298 of `LinkOpmodeDistributionImporter.java`).
pub fn validate_op_mode_distribution(
    op_mode_distribution: &RecordBatch,
    expected_source_types: &BTreeSet<i64>,
    expected_hour_days: &BTreeSet<i64>,
) -> Result<()> {
    let source_type = column_i64(op_mode_distribution, "sourceTypeID")?;
    let hour_day = column_i64(op_mode_distribution, "hourDayID")?;
    let mut seen_st = BTreeSet::new();
    let mut seen_hd = BTreeSet::new();
    for i in 0..op_mode_distribution.num_rows() {
        if !source_type.is_null(i) {
            seen_st.insert(source_type.value(i));
        }
        if !hour_day.is_null(i) {
            seen_hd.insert(hour_day.value(i));
        }
    }
    coverage_check(
        "OpModeDistribution",
        "sourceTypeID",
        expected_source_types,
        &seen_st,
    )?;
    coverage_check(
        "OpModeDistribution",
        "hourDayID",
        expected_hour_days,
        &seen_hd,
    )?;
    Ok(())
}

// ------------------------------------------------------------------------

fn coverage_check(
    table: &str,
    column: &str,
    expected: &BTreeSet<i64>,
    seen: &BTreeSet<i64>,
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    let missing: Vec<i64> = expected.difference(seen).copied().collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Error::Validation {
            table: table.into(),
            message: format!("{table} is missing {column}(s): {missing:?}",),
        })
    }
}

fn column_i64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Validation {
            table: "(unknown)".into(),
            message: format!("column '{name}' not found in batch"),
        })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Validation {
            table: "(unknown)".into(),
            message: format!("column '{name}' is not Int64"),
        })
}

fn column_f64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Validation {
            table: "(unknown)".into(),
            message: format!("column '{name}' not found in batch"),
        })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Validation {
            table: "(unknown)".into(),
            message: format!("column '{name}' is not Float64"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_reader::read_csv_from_reader;
    use crate::filter::RunSpecFilter;
    use crate::schema::{LINK, LINK_SOURCE_TYPE_HOUR, OFF_NETWORK_LINK, OP_MODE_DISTRIBUTION};
    use std::io::Cursor;
    use std::path::Path;

    fn batch(csv: &str, schema: &crate::schema::TableSchema) -> RecordBatch {
        read_csv_from_reader(
            Cursor::new(csv.as_bytes()),
            Path::new("(test)"),
            schema,
            &RunSpecFilter::default(),
        )
        .unwrap()
        .batch
    }

    #[test]
    fn fraction_sum_passes_when_unity() {
        let lsth = batch(
            "linkID,sourceTypeID,sourceTypeHourFraction\n1,21,0.5\n1,32,0.5\n",
            &LINK_SOURCE_TYPE_HOUR,
        );
        validate_link_source_type_hour(&lsth, None).unwrap();
    }

    #[test]
    fn fraction_sum_fails_when_off_unity() {
        let lsth = batch(
            "linkID,sourceTypeID,sourceTypeHourFraction\n1,21,0.4\n1,32,0.5\n",
            &LINK_SOURCE_TYPE_HOUR,
        );
        let err = validate_link_source_type_hour(&lsth, None).unwrap_err();
        match err {
            Error::Validation { message, .. } => {
                assert!(message.contains("0.9"), "got: {message}");
                assert!(message.contains("linkID 1"), "got: {message}");
            }
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn off_network_linkid_not_allowed_in_lsth() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
99,26161,261610,1,0,0,0,offnet,
1,26161,261610,4,0.5,1000,55,onnet,0
",
            &LINK,
        );
        let lsth = batch(
            "linkID,sourceTypeID,sourceTypeHourFraction\n1,21,1.0\n99,21,1.0\n",
            &LINK_SOURCE_TYPE_HOUR,
        );
        let err = validate_link_source_type_hour(&lsth, Some(&link)).unwrap_err();
        match err {
            Error::Validation { message, .. } => {
                assert!(message.contains("linkID 99"), "got: {message}");
                assert!(message.contains("off-network"), "got: {message}");
            }
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn missing_onnetwork_link_in_lsth_fails() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,onnet1,0
2,26161,261610,4,0.5,1000,45,onnet2,0
",
            &LINK,
        );
        let lsth = batch(
            "linkID,sourceTypeID,sourceTypeHourFraction\n1,21,1.0\n",
            &LINK_SOURCE_TYPE_HOUR,
        );
        let err = validate_link_source_type_hour(&lsth, Some(&link)).unwrap_err();
        match err {
            Error::Validation { message, .. } => {
                assert!(message.contains("linkID 2"), "got: {message}");
                assert!(message.contains("missing"), "got: {message}");
            }
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn missing_offnetwork_link_in_lsth_is_ok() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
99,26161,261610,1,0,0,0,offnet,
1,26161,261610,4,0.5,1000,55,onnet,0
",
            &LINK,
        );
        let lsth = batch(
            "linkID,sourceTypeID,sourceTypeHourFraction\n1,21,1.0\n",
            &LINK_SOURCE_TYPE_HOUR,
        );
        // off-network link 99 is excluded from lsth; that's correct.
        validate_link_source_type_hour(&lsth, Some(&link)).unwrap();
    }

    #[test]
    fn link_coverage_passes_when_all_present() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,1.0,100,50,a,
2,26161,261610,2,1.0,100,50,b,
",
            &LINK,
        );
        let counties = BTreeSet::from([26161]);
        let zones = BTreeSet::from([261610]);
        let road_types = BTreeSet::from([2, 4]);
        validate_link(&link, &counties, &zones, &road_types).unwrap();
    }

    #[test]
    fn link_coverage_reports_missing_road_type() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,1.0,100,50,a,
",
            &LINK,
        );
        let counties = BTreeSet::from([26161]);
        let zones = BTreeSet::from([261610]);
        let road_types = BTreeSet::from([2, 4]);
        let err = validate_link(&link, &counties, &zones, &road_types).unwrap_err();
        match err {
            Error::Validation { message, .. } => assert!(message.contains("[2]"), "got: {message}"),
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn off_network_coverage_succeeds() {
        let off = batch(
            "zoneID,sourceTypeID,vehiclePopulation,startFraction,extendedIdleFraction,parkedVehicleFraction
261610,21,1000,0.1,0.0,0.9
261610,32,500,0.05,0.2,0.75
",
            &OFF_NETWORK_LINK,
        );
        let st = BTreeSet::from([21, 32]);
        validate_off_network_link(&off, &st).unwrap();
    }

    #[test]
    fn op_mode_coverage_reports_missing_hour_day() {
        let opmd = batch(
            "sourceTypeID,hourDayID,linkID,polProcessID,opModeID,opModeFraction
21,55,1,1101,0,1.0
",
            &OP_MODE_DISTRIBUTION,
        );
        let st = BTreeSet::from([21]);
        let hd = BTreeSet::from([55, 75]);
        let err = validate_op_mode_distribution(&opmd, &st, &hd).unwrap_err();
        match err {
            Error::Validation { message, .. } => {
                assert!(message.contains("hourDayID"), "got: {message}");
                assert!(message.contains("75"), "got: {message}");
            }
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn empty_expected_set_skips_check() {
        let link = batch(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,1.0,100,50,a,
",
            &LINK,
        );
        validate_link(&link, &BTreeSet::new(), &BTreeSet::new(), &BTreeSet::new()).unwrap();
    }
}

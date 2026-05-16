//! `ZoneImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/ZoneImporter.java`.
//!
//! `ZoneImporter` is the first multi-table importer in the CDB set: it
//! manages both `zone` (zone-level allocation factors) and
//! `zoneRoadType` (zone × road-type SHO factors). The Java
//! `dataTableDescriptor` lists them back-to-back, each prefixed with
//! `BasicDataHandler.BEGIN_TABLE`. We declare two
//! [`TableDescriptor`]s and the [`ZoneImporter::tables`] method
//! returns both.
//!
//! Cross-row invariants:
//!
//! * `zone.startAllocFactor` must sum to 1.0 per `countyID`.
//! * `zone.idleAllocFactor` must sum to 1.0 per `countyID`.
//! * `zone.SHPAllocFactor` must sum to 1.0 per `countyID`.
//! * `zoneRoadType.SHOAllocFactor` must sum to 1.0 per (countyID, roadTypeID).
//!
//! These are not in the Java SQL file (`ZoneImporter.sql` is absent
//! from the canonical tree we depend on) but the constraints are
//! documented in `DebuggingMOVES.md` and the corresponding generator
//! code in `gov/epa/otaq/moves/master/implementation/general/...`.

use std::collections::BTreeMap;

use arrow::array::{Array, Float64Array, Int64Array};
use moves_importer::{
    ColumnDescriptor, Filter, ImportedTable, Importer, TableDescriptor, ValidationContext,
    ValidationMessage,
};

const ZONE_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("countyID", Filter::County),
    ColumnDescriptor::new("startAllocFactor", Filter::NonNegative),
    ColumnDescriptor::new("idleAllocFactor", Filter::NonNegative),
    ColumnDescriptor::new("SHPAllocFactor", Filter::NonNegative),
];

const ZONE_ROAD_TYPE_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("roadTypeID", Filter::RoadTypeNotOffNetwork),
    ColumnDescriptor::new("SHOAllocFactor", Filter::NonNegative),
];

const ZONE_TABLE: TableDescriptor = TableDescriptor {
    name: "Zone",
    columns: ZONE_COLUMNS,
    primary_key: &["zoneID"],
};

const ZONE_ROAD_TYPE_TABLE: TableDescriptor = TableDescriptor {
    name: "ZoneRoadType",
    columns: ZONE_ROAD_TYPE_COLUMNS,
    primary_key: &["zoneID", "roadTypeID"],
};

const TABLES: &[TableDescriptor] = &[ZONE_TABLE, ZONE_ROAD_TYPE_TABLE];

/// Composite zone importer covering `zone` and `zoneRoadType`.
#[derive(Debug, Default)]
pub struct ZoneImporter;

impl Importer for ZoneImporter {
    fn name(&self) -> &'static str {
        "Zone"
    }
    fn xml_node_type(&self) -> &'static str {
        "zone"
    }
    fn tables(&self) -> &'static [TableDescriptor] {
        TABLES
    }

    fn validate_imported(
        &self,
        tables: &[ImportedTable<'_>],
        _ctx: &ValidationContext<'_>,
    ) -> Vec<ValidationMessage> {
        let mut out = Vec::new();
        check_zone_table(&tables[0], &mut out);
        check_zone_road_type_table(&tables[0], &tables[1], &mut out);
        out
    }
}

fn check_zone_table(zone: &ImportedTable<'_>, out: &mut Vec<ValidationMessage>) {
    let batch = &zone.batch;
    let county = batch
        .column_by_name("countyID")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
    let start = batch
        .column_by_name("startAllocFactor")
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>());
    let idle = batch
        .column_by_name("idleAllocFactor")
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>());
    let shp = batch
        .column_by_name("SHPAllocFactor")
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>());
    let (Some(county), Some(start), Some(idle), Some(shp)) = (county, start, idle, shp) else {
        return;
    };

    let mut sums: BTreeMap<i64, (f64, f64, f64)> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if county.is_null(row) {
            continue;
        }
        let c = county.value(row);
        let entry = sums.entry(c).or_insert((0.0, 0.0, 0.0));
        if !start.is_null(row) {
            entry.0 += start.value(row);
        }
        if !idle.is_null(row) {
            entry.1 += idle.value(row);
        }
        if !shp.is_null(row) {
            entry.2 += shp.value(row);
        }
    }

    for (county_id, (s, i, h)) in sums {
        push_off_balance(out, "Zone", "startAllocFactor", county_id, s);
        push_off_balance(out, "Zone", "idleAllocFactor", county_id, i);
        push_off_balance(out, "Zone", "SHPAllocFactor", county_id, h);
    }
}

fn check_zone_road_type_table(
    zone: &ImportedTable<'_>,
    zone_road_type: &ImportedTable<'_>,
    out: &mut Vec<ValidationMessage>,
) {
    // Join zoneRoadType.zoneID to Zone.countyID so the per-county sum
    // matches the canonical MOVES check from `DebuggingMOVES.md`.
    let zone_batch = &zone.batch;
    let zrt_batch = &zone_road_type.batch;
    let zid_in_zone = zone_batch
        .column_by_name("zoneID")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
    let cid_in_zone = zone_batch
        .column_by_name("countyID")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
    let zid_in_zrt = zrt_batch
        .column_by_name("zoneID")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
    let road_in_zrt = zrt_batch
        .column_by_name("roadTypeID")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
    let factor_in_zrt = zrt_batch
        .column_by_name("SHOAllocFactor")
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>());
    let (Some(zid_z), Some(cid_z), Some(zid_zrt), Some(road), Some(factor)) = (
        zid_in_zone,
        cid_in_zone,
        zid_in_zrt,
        road_in_zrt,
        factor_in_zrt,
    ) else {
        return;
    };

    let mut zone_to_county: BTreeMap<i64, i64> = BTreeMap::new();
    for row in 0..zone_batch.num_rows() {
        if zid_z.is_null(row) || cid_z.is_null(row) {
            continue;
        }
        zone_to_county.insert(zid_z.value(row), cid_z.value(row));
    }

    let mut sums: BTreeMap<(i64, i64), f64> = BTreeMap::new();
    for row in 0..zrt_batch.num_rows() {
        if zid_zrt.is_null(row) || road.is_null(row) || factor.is_null(row) {
            continue;
        }
        let zone_id = zid_zrt.value(row);
        let Some(&county_id) = zone_to_county.get(&zone_id) else {
            out.push(ValidationMessage::error(
                "ZoneRoadType",
                Some("zoneID"),
                Some(row + 2),
                format!("zoneID {zone_id} is not present in the Zone table"),
            ));
            continue;
        };
        let r = road.value(row);
        *sums.entry((county_id, r)).or_insert(0.0) += factor.value(row);
    }

    for ((county_id, road_type_id), sum) in sums {
        let rounded = (sum * 10_000.0).round() / 10_000.0;
        if (rounded - 1.0).abs() > f64::EPSILON {
            out.push(ValidationMessage::error(
                "ZoneRoadType",
                Some("SHOAllocFactor"),
                None,
                format!(
                    "countyID {county_id} roadTypeID {road_type_id} SHOAllocFactor sums to {rounded:.4}, expected 1.0000"
                ),
            ));
        }
    }
}

fn push_off_balance(
    out: &mut Vec<ValidationMessage>,
    table: &'static str,
    column: &'static str,
    county_id: i64,
    sum: f64,
) {
    let rounded = (sum * 10_000.0).round() / 10_000.0;
    if (rounded - 1.0).abs() > f64::EPSILON {
        out.push(ValidationMessage::error(
            table,
            Some(column),
            None,
            format!("countyID {county_id} {column} sums to {rounded:.4}, expected 1.0000"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn zone_batch(rows: &[(i64, i64, f64, f64, f64)]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Float64Builder, Int64Builder};
        let mut z = Int64Builder::new();
        let mut c = Int64Builder::new();
        let mut s = Float64Builder::new();
        let mut i = Float64Builder::new();
        let mut h = Float64Builder::new();
        for &(zone, county, start, idle, shp) in rows {
            z.append_value(zone);
            c.append_value(county);
            s.append_value(start);
            i.append_value(idle);
            h.append_value(shp);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(z.finish()),
            Arc::new(c.finish()),
            Arc::new(s.finish()),
            Arc::new(i.finish()),
            Arc::new(h.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(&ZONE_TABLE, cols).unwrap()
    }

    fn zrt_batch(rows: &[(i64, i64, f64)]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Float64Builder, Int64Builder};
        let mut z = Int64Builder::new();
        let mut r = Int64Builder::new();
        let mut f = Float64Builder::new();
        for &(zone, road, factor) in rows {
            z.append_value(zone);
            r.append_value(road);
            f.append_value(factor);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(z.finish()),
            Arc::new(r.finish()),
            Arc::new(f.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(&ZONE_ROAD_TYPE_TABLE, cols)
            .unwrap()
    }

    #[test]
    fn balanced_zone_factors_pass() {
        // One county (60371) with one zone (603710) holding all the
        // allocation — the trivial balanced case.
        let z = zone_batch(&[(603710, 60371, 1.0, 1.0, 1.0)]);
        let r = zrt_batch(&[
            (603710, 2, 1.0),
            (603710, 3, 1.0),
            (603710, 4, 1.0),
            (603710, 5, 1.0),
        ]);
        let imp = ZoneImporter;
        let z_imp = ImportedTable::new(&imp.tables()[0], PathBuf::from("zone.csv"), z);
        let r_imp = ImportedTable::new(&imp.tables()[1], PathBuf::from("zoneroadtype.csv"), r);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[z_imp, r_imp], &ctx);
        assert!(msgs.is_empty(), "got: {msgs:?}");
    }

    #[test]
    fn off_balance_zone_factor_is_flagged_per_county() {
        let z = zone_batch(&[
            (603710, 60371, 0.5, 1.0, 1.0),
            (603711, 60371, 0.4, 0.0, 0.0),
            // startAllocFactor sums to 0.9 (off) for county 60371
        ]);
        let r = zrt_batch(&[(603710, 2, 1.0)]);
        let imp = ZoneImporter;
        let z_imp = ImportedTable::new(&imp.tables()[0], PathBuf::from("zone.csv"), z);
        let r_imp = ImportedTable::new(&imp.tables()[1], PathBuf::from("zoneroadtype.csv"), r);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[z_imp, r_imp], &ctx);
        let errors: Vec<_> = msgs.iter().filter(|m| m.is_error()).collect();
        // startAllocFactor over county 60371 sums to 0.9 — fail
        // idleAllocFactor over county 60371 sums to 1.0 — ok
        // SHPAllocFactor over county 60371 sums to 1.0 — ok
        // ZoneRoadType.SHOAllocFactor for (60371, 2) sums to 1.0 — ok
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert_eq!(errors[0].column, Some("startAllocFactor"));
    }

    #[test]
    fn unknown_zone_id_in_zone_road_type_is_an_error() {
        let z = zone_batch(&[(603710, 60371, 1.0, 1.0, 1.0)]);
        let r = zrt_batch(&[
            (603710, 2, 1.0),
            // zoneID 999 not declared in the Zone table
            (999, 2, 1.0),
        ]);
        let imp = ZoneImporter;
        let z_imp = ImportedTable::new(&imp.tables()[0], PathBuf::from("zone.csv"), z);
        let r_imp = ImportedTable::new(&imp.tables()[1], PathBuf::from("zoneroadtype.csv"), r);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[z_imp, r_imp], &ctx);
        let zone_errors: Vec<_> = msgs
            .iter()
            .filter(|m| m.column == Some("zoneID") && m.is_error())
            .collect();
        assert_eq!(zone_errors.len(), 1);
        assert!(zone_errors[0].message.contains("999"));
    }
}

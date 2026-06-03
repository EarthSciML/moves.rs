//! `ZoneRoadTypeImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/ZoneRoadTypeImporter.java`.
//!
//! Single table `ZoneRoadType` with three columns:
//!
//! | column | filter |
//! |---|---|
//! | `zoneID` | [`Filter::Zone`] |
//! | `roadTypeID` | [`Filter::RoadType`] |
//! | `SHOAllocFactor` | [`Filter::NonNegative`] |
//!
//! The matching SQL script `database/ZoneRoadTypeImporter.sql`
//! enforces a single cross-row invariant: `SHOAllocFactor` must sum to
//! exactly 1.0 (within four-decimal rounding) for every `roadTypeID`.
//! See [`ZoneRoadTypeImporter::validate_imported`] for the Rust port
//! of that check.

use arrow::array::{Array, Float64Array, Int64Array};
use moves_importer::{
    ColumnDescriptor, Filter, ImportedTable, Importer, TableDescriptor, ValidationContext,
    ValidationMessage,
};

const COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("roadTypeID", Filter::RoadType),
    ColumnDescriptor::new("SHOAllocFactor", Filter::NonNegative),
];

const TABLE: TableDescriptor = TableDescriptor {
    name: "ZoneRoadType",
    columns: COLUMNS,
    primary_key: &["zoneID", "roadTypeID"],
};

const TABLES: &[TableDescriptor] = &[TABLE];

/// Zone × road-type SHO allocation importer.
#[derive(Debug, Default)]
pub struct ZoneRoadTypeImporter;

impl Importer for ZoneRoadTypeImporter {
    fn name(&self) -> &'static str {
        "Zone Road Activity"
    }
    fn xml_node_type(&self) -> &'static str {
        "zoneroadtype"
    }
    fn tables(&self) -> &'static [TableDescriptor] {
        TABLES
    }

    fn validate_imported(
        &self,
        tables: &[ImportedTable<'_>],
        _ctx: &ValidationContext<'_>,
    ) -> Vec<ValidationMessage> {
        let imported = &tables[0];
        let batch = &imported.batch;
        let road = batch
            .column_by_name("roadTypeID")
            .expect("descriptor lists roadTypeID")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("roadTypeID is Int64 per filter");
        let factor = batch
            .column_by_name("SHOAllocFactor")
            .expect("descriptor lists SHOAllocFactor")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("SHOAllocFactor is Float64 per filter");

        let mut sums: std::collections::BTreeMap<i64, f64> = std::collections::BTreeMap::new();
        for row in 0..batch.num_rows() {
            if road.is_null(row) || factor.is_null(row) {
                continue;
            }
            let r = road.value(row);
            let f = factor.value(row);
            *sums.entry(r).or_insert(0.0) += f;
        }

        let mut out = Vec::new();
        for (road_type, sum) in sums {
            // Round to 4 decimal places, matching MariaDB's
            // `round(sum(SHOAllocFactor),4) <> 1.0000` check.
            let rounded = (sum * 10_000.0).round() / 10_000.0;
            if (rounded - 1.0).abs() > f64::EPSILON {
                out.push(ValidationMessage::error(
                    "ZoneRoadType",
                    Some("SHOAllocFactor"),
                    None,
                    format!(
                        "Road type {road_type} SHOAllocFactor is not 1.0 but instead {rounded:.4}"
                    ),
                ));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn batch(factors: &[(i64, i64, f64)]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Float64Builder, Int64Builder};
        let mut z = Int64Builder::new();
        let mut r = Int64Builder::new();
        let mut f = Float64Builder::new();
        for &(zone, road, factor) in factors {
            z.append_value(zone);
            r.append_value(road);
            f.append_value(factor);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(z.finish()),
            Arc::new(r.finish()),
            Arc::new(f.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(&TABLE, cols).unwrap()
    }

    #[test]
    fn factors_summing_to_one_per_road_type_pass() {
        // Two zones in county 6037; for each road type the zones'
        // SHOAllocFactor sums to exactly 1.0, which is the invariant
        // `validate_imported` enforces per `roadTypeID`.
        let b = batch(&[
            (60371, 2, 0.60),
            (60372, 2, 0.40),
            (60371, 3, 0.70),
            (60372, 3, 0.30),
            (60371, 4, 0.55),
            (60372, 4, 0.45),
            (60371, 5, 0.25),
            (60372, 5, 0.75),
        ]);
        let imp = ZoneRoadTypeImporter;
        let imported = ImportedTable::new(&imp.tables()[0], PathBuf::from("test"), b);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[imported], &ctx);
        assert!(msgs.is_empty(), "got: {msgs:?}");
    }

    #[test]
    fn road_type_whose_factors_do_not_sum_to_one_is_flagged() {
        let b = batch(&[
            (60371, 2, 0.25),
            (60371, 3, 0.30),
            // road type 4 sums to 1.10 — over
            (60371, 4, 0.10),
            (60372, 4, 1.00),
            (60371, 5, 0.35),
        ]);
        let imp = ZoneRoadTypeImporter;
        let imported = ImportedTable::new(&imp.tables()[0], PathBuf::from("test"), b);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[imported], &ctx);
        let errors: Vec<_> = msgs.iter().filter(|m| m.is_error()).collect();
        // Road type 4 sums to 1.10, road type 2 sums to 0.25 only,
        // road type 3 sums to 0.30 only, road type 5 sums to 0.35
        // only — so four error messages, one per off-balance road type.
        assert_eq!(errors.len(), 4, "got: {errors:?}");
        assert!(errors
            .iter()
            .all(|m| m.column == Some("SHOAllocFactor") && m.row.is_none()));
    }

    #[test]
    fn rounding_to_four_decimals_tolerates_micro_drift() {
        // Road type 2 across three zones: 0.3333 + 0.3333 + 0.3334
        // rounds to exactly 1.0000, so the four-decimal tolerance
        // accepts it.
        let b = batch(&[(60371, 2, 0.3333), (60372, 2, 0.3333), (60373, 2, 0.3334)]);
        let imp = ZoneRoadTypeImporter;
        let imported = ImportedTable::new(&imp.tables()[0], PathBuf::from("test"), b);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[imported], &ctx);
        assert!(msgs.is_empty(), "got: {msgs:?}");
    }
}

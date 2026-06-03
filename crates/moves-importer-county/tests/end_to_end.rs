//! End-to-end tests: read a CSV, run the framework + importer
//! validators, write the Parquet output, and re-read the Parquet to
//! confirm round-trip values match.
//!
//! These tests don't depend on a default-DB snapshot. They use
//! [`ValidationContext::without_default_db`] which downgrades FK
//! filters to warnings — sufficient for exercising the numeric and
//! cross-row paths in CI without staging a full default-DB tree.

use std::io::Write;

use moves_importer::Importer;
use moves_importer::{
    read_csv_table, validate_table, write_table_parquet, ImportedTable, Severity, ValidationContext,
};
use moves_importer_county::{
    AgeDistributionImporter, SourceTypePopulationImporter, ZoneRoadTypeImporter,
};

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[test]
fn source_type_population_csv_to_parquet_round_trip() {
    let csv = "\
yearID,sourceTypeID,sourceTypePopulation
2020,21,1500000
2020,32,50000
2021,21,1525000
2021,32,52000
";
    let f = write_csv(csv);
    let importer = SourceTypePopulationImporter;
    let descriptor = &importer.tables()[0];
    let rows = read_csv_table(f.path(), descriptor).unwrap();
    let imported = ImportedTable::new(descriptor, rows.source_path, rows.batch);

    let ctx = ValidationContext::without_default_db();
    let column_msgs = validate_table(&imported, &ctx).unwrap();
    let cross_msgs = importer.validate_imported(std::slice::from_ref(&imported), &ctx);

    let errors: Vec<_> = column_msgs
        .iter()
        .chain(cross_msgs.iter())
        .filter(|m| m.is_error())
        .collect();
    assert!(errors.is_empty(), "got: {errors:?}");

    let out = write_table_parquet(descriptor, &imported.batch, None).unwrap();
    assert_eq!(out.row_count, 4);
    assert_eq!(out.table_name, "SourceTypeYear");
    assert!(!out.sha256.is_empty());

    // Re-reading the bytes with Parquet must reproduce the same values.
    let cursor = bytes::Bytes::from(out.bytes);
    let arrow_reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(cursor)
            .unwrap()
            .build()
            .unwrap();
    let batches: Vec<_> = arrow_reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 4);

    // Verify rows are sorted lexicographically by primary key
    // (yearID asc, sourceTypeID asc).
    let year = b
        .column_by_name("yearID")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let source_type = b
        .column_by_name("sourceTypeID")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let keys: Vec<(i64, i64)> = (0..b.num_rows())
        .map(|i| (year.value(i), source_type.value(i)))
        .collect();
    assert_eq!(keys, vec![(2020, 21), (2020, 32), (2021, 21), (2021, 32)]);
}

#[test]
fn zone_road_type_off_balance_rejected_by_cross_row_check() {
    let csv = "\
zoneID,roadTypeID,SHOAllocFactor
603710,2,0.30
603710,3,0.30
603710,4,0.30
603710,5,0.30
";
    let f = write_csv(csv);
    let importer = ZoneRoadTypeImporter;
    let descriptor = &importer.tables()[0];
    let rows = read_csv_table(f.path(), descriptor).unwrap();
    let imported = ImportedTable::new(descriptor, rows.source_path, rows.batch);

    let ctx = ValidationContext::without_default_db();
    let cross_msgs = importer.validate_imported(&[imported], &ctx);
    // Every road type's column sums to 0.30 across one zone, so every
    // road type fails the sum-to-1 invariant. Should be 4 errors.
    let errors: Vec<_> = cross_msgs.iter().filter(|m| m.is_error()).collect();
    assert_eq!(errors.len(), 4, "got: {errors:?}");
}

#[test]
fn age_distribution_balanced_passes_and_writes_sorted_parquet() {
    let csv = "\
sourceTypeID,yearID,ageID,ageFraction
21,2020,2,0.20
21,2020,0,0.50
21,2020,1,0.30
";
    let f = write_csv(csv);
    let importer = AgeDistributionImporter;
    let descriptor = &importer.tables()[0];
    let rows = read_csv_table(f.path(), descriptor).unwrap();
    let imported = ImportedTable::new(descriptor, rows.source_path, rows.batch);

    let ctx = ValidationContext::without_default_db();
    let column_msgs = validate_table(&imported, &ctx).unwrap();
    let cross_msgs = importer.validate_imported(std::slice::from_ref(&imported), &ctx);
    let errors: Vec<_> = column_msgs
        .iter()
        .chain(cross_msgs.iter())
        .filter(|m| m.is_error())
        .collect();
    assert!(errors.is_empty(), "got: {errors:?}");

    let out = write_table_parquet(descriptor, &imported.batch, None).unwrap();
    assert_eq!(out.row_count, 3);

    let cursor = bytes::Bytes::from(out.bytes);
    let batches: Vec<_> =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(cursor)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let b = &batches[0];
    let age = b
        .column_by_name("ageID")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    // Primary key is (sourceTypeID, yearID, ageID). Same sourceTypeID
    // and yearID across all rows, so ages come out in ascending order.
    assert_eq!(
        (0..b.num_rows()).map(|i| age.value(i)).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[test]
fn fk_filters_warn_without_default_db_so_tests_can_be_isolated() {
    let csv = "\
yearID,sourceTypeID,sourceTypePopulation
2020,21,1500000
";
    let f = write_csv(csv);
    let importer = SourceTypePopulationImporter;
    let descriptor = &importer.tables()[0];
    let rows = read_csv_table(f.path(), descriptor).unwrap();
    let imported = ImportedTable::new(descriptor, rows.source_path, rows.batch);

    let ctx = ValidationContext::without_default_db();
    let msgs = validate_table(&imported, &ctx).unwrap();
    let warnings: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(m.severity, Severity::Warning))
        .collect();
    // yearID and sourceTypeID are FK columns; each emits one warning.
    assert_eq!(warnings.len(), 2);
}

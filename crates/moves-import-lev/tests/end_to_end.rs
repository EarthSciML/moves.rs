//! End-to-end integration tests for the LEV/NLEV importer.
//!
//! These tests drive the public API (`import_lev` / `import_nlev`)
//! against fixture CSVs on disk and verify the resulting Parquet
//! round-trips back to the same row values.

use std::path::Path;

use arrow::array::{Array, Float64Array, Int64Array};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use moves_import_lev::{import_lev, import_nlev, parquet_path_for, LevKind, COLUMNS};

fn read_parquet(path: &Path) -> arrow::record_batch::RecordBatch {
    let bytes = std::fs::read(path).unwrap();
    let bytes = Bytes::from(bytes);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    reader.next().unwrap().unwrap()
}

const SAMPLE_CSV: &[u8] = b"# Sample LEV input fixture for end-to-end tests.\n\
sourceBinID,polProcessID,opModeID,ageGroupID,meanBaseRate,meanBaseRateCV,meanBaseRateIM,meanBaseRateIMCV,dataSourceId\n\
1000123456789,101,1,1,0.012500,0.05,0.014000,0.05,1\n\
1000123456789,101,1,2,0.018000,0.05,0.020000,0.05,1\n\
1000123456789,101,11,1,0.005000,,,,\n\
2000000000001,201,1,1,0,,,,99\n\
";

fn write_fixture(dir: &Path, name: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, SAMPLE_CSV).unwrap();
    p
}

#[test]
fn imports_lev_csv_to_parquet_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = write_fixture(tmp.path(), "lev.csv");
    let out_dir = tmp.path().join("parquet");

    let report = import_lev(&csv_path, &out_dir).unwrap();

    assert_eq!(report.kind, LevKind::Lev);
    assert_eq!(report.row_count, 4);
    assert_eq!(report.input_path, csv_path);
    assert_eq!(report.output_path, parquet_path_for(LevKind::Lev, &out_dir));
    assert!(report.output_path.exists());
    assert_eq!(report.sha256.len(), 64);
    assert!(report.sha256.chars().all(|c| c.is_ascii_hexdigit()));

    // Round-trip: read Parquet back and verify all columns survived.
    let batch = read_parquet(&report.output_path);
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.num_columns(), COLUMNS.len());

    let source_bin = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(source_bin.value(0), 1_000_123_456_789);
    assert_eq!(source_bin.value(3), 2_000_000_000_001);

    let pol_proc = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pol_proc.value(0), 101);
    assert_eq!(pol_proc.value(3), 201);

    let mean_base_rate = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(mean_base_rate.value(0), 0.0125);
    assert_eq!(mean_base_rate.value(3), 0.0);

    let mean_base_rate_cv = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(mean_base_rate_cv.value(0), 0.05);
    // Row 2 (index 2) has blank optional cells → null.
    assert!(mean_base_rate_cv.is_null(2));

    let data_source = batch
        .column(8)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(data_source.value(0), 1);
    // Row 2 has blank dataSourceId.
    assert!(data_source.is_null(2));
    assert_eq!(data_source.value(3), 99);
}

#[test]
fn imports_nlev_csv_writes_to_nlev_table_path() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = write_fixture(tmp.path(), "nlev.csv");
    let out_dir = tmp.path().join("parquet");

    let report = import_nlev(&csv_path, &out_dir).unwrap();

    assert_eq!(report.kind, LevKind::Nlev);
    assert_eq!(
        report.output_path,
        parquet_path_for(LevKind::Nlev, &out_dir)
    );
    assert!(report.output_path.to_string_lossy().contains("NLEV"));
    let batch = read_parquet(&report.output_path);
    assert_eq!(batch.num_rows(), 4);
}

#[test]
fn lev_and_nlev_with_same_csv_produce_same_hash() {
    // The Parquet content is independent of which kind it represents;
    // both reuse the same writer settings.
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = write_fixture(tmp.path(), "shared.csv");
    let out_dir = tmp.path().join("parquet");

    let r_lev = import_lev(&csv_path, &out_dir.join("lev")).unwrap();
    let r_nlev = import_nlev(&csv_path, &out_dir.join("nlev")).unwrap();
    assert_eq!(r_lev.sha256, r_nlev.sha256);
}

#[test]
fn rerunning_same_csv_yields_byte_identical_output() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = write_fixture(tmp.path(), "lev.csv");

    let out_a = tmp.path().join("a");
    let out_b = tmp.path().join("b");
    let a = import_lev(&csv_path, &out_a).unwrap();
    let b = import_lev(&csv_path, &out_b).unwrap();
    assert_eq!(a.sha256, b.sha256);
    let bytes_a = std::fs::read(&a.output_path).unwrap();
    let bytes_b = std::fs::read(&b.output_path).unwrap();
    assert_eq!(bytes_a, bytes_b);
}

#[test]
fn missing_csv_returns_io_error() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("does-not-exist.csv");
    let err = import_lev(&csv_path, tmp.path()).unwrap_err();
    assert!(matches!(err, moves_import_lev::Error::Io { .. }), "{err:?}");
}

#[test]
fn malformed_csv_surfaces_validation_error() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("bad.csv");
    // Missing ageGroupID (required).
    std::fs::write(
        &csv_path,
        b"sourceBinID,polProcessID,opModeID,meanBaseRate\n1000,101,1,0.5\n",
    )
    .unwrap();
    let err = import_lev(&csv_path, tmp.path()).unwrap_err();
    match err {
        moves_import_lev::Error::MissingRequiredColumn { column, .. } => {
            assert_eq!(column, "ageGroupID");
        }
        other => panic!("got {other:?}"),
    }
}

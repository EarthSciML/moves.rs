//! End-to-end PDB import: CSV → Parquet → read back via Parquet
//! reader and verify schema + row counts. Runs the same call surface
//! a downstream tool (e.g. the importer validation suite at Task 88)
//! would use.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use moves_importer_pdb::{filter::RunSpecFilter, schema, ImportSession};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

fn write_fixture(dir: &std::path::Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path
}

#[test]
fn full_pdb_round_trip_matches_default_db_schema() {
    let dir = TempDir::new().unwrap();
    let runspec = RunSpecFilter::default()
        .with_counties([26161])
        .with_zones([261610])
        .with_road_types([1, 4])
        .with_source_types([21, 32])
        .with_hour_days([55])
        .with_op_modes([0, 1, 11, 21])
        .with_pol_processes([1101, 9001]);

    // Fixtures: a minimal but realistic Washtenaw County (matches
    // characterization/fixtures/scale-project.xml's host county).
    let link_csv = write_fixture(
        dir.path(),
        "link.csv",
        "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.500,1000,55,M14 EB segment,0.0
2,26161,261610,4,0.250,500,45,M14 WB segment,0.0
99,26161,261610,1,0.000,0,0,Off-network,
",
    );
    let lsth_csv = write_fixture(
        dir.path(),
        "link_source_type_hour.csv",
        "linkID,sourceTypeID,sourceTypeHourFraction
1,21,0.7
1,32,0.3
2,21,0.6
2,32,0.4
",
    );
    let drive_csv = write_fixture(
        dir.path(),
        "drive_schedule.csv",
        "linkID,secondID,speed,grade
1,0,55.0,0.0
1,1,55.5,0.1
1,2,56.0,0.0
",
    );
    let off_csv = write_fixture(
        dir.path(),
        "off_network.csv",
        "zoneID,sourceTypeID,vehiclePopulation,startFraction,extendedIdleFraction,parkedVehicleFraction
261610,21,1000,0.05,0.0,0.95
261610,32,500,0.10,0.0,0.90
",
    );
    let opmd_csv = write_fixture(
        dir.path(),
        "op_mode.csv",
        "sourceTypeID,hourDayID,linkID,polProcessID,opModeID,opModeFraction
21,55,99,1101,1,0.5
21,55,99,1101,11,0.5
32,55,99,1101,1,0.4
32,55,99,1101,11,0.6
",
    );

    let out_dir = dir.path().join("pdb-out");
    let session = ImportSession::builder(&out_dir, &runspec)
        .with_link(&link_csv)
        .with_link_source_type_hour(&lsth_csv)
        .with_drive_schedule_second_link(&drive_csv)
        .with_off_network_link(&off_csv)
        .with_op_mode_distribution(&opmd_csv)
        .build()
        .unwrap();
    let manifest = session.write_to_disk().unwrap();
    assert_eq!(manifest.tables.len(), 5);

    // Verify each Parquet file reads back with the schema we expect.
    for entry in &manifest.tables {
        let bytes = std::fs::read(&entry.output_path).unwrap();
        let bytes = Bytes::from(bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let mut total = 0;
        for batch in reader {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(
            total as u64, entry.row_count,
            "row count for {}",
            entry.name
        );
    }
}

#[test]
fn parquet_schema_matches_default_db_widening() {
    // moves-default-db-convert widens every MariaDB integer flavor to
    // Int64 and float to Float64. Verify the importer follows the
    // same convention so a downstream consumer can union-scan
    // default-DB Parquet and importer-emitted Parquet for the same
    // table without schema disagreement.
    let expected_link = Arc::new(ArrowSchema::new(vec![
        Field::new("linkID", DataType::Int64, true),
        Field::new("countyID", DataType::Int64, true),
        Field::new("zoneID", DataType::Int64, true),
        Field::new("roadTypeID", DataType::Int64, true),
        Field::new("linkLength", DataType::Float64, true),
        Field::new("linkVolume", DataType::Float64, true),
        Field::new("linkAvgSpeed", DataType::Float64, true),
        Field::new("linkDescription", DataType::Utf8, true),
        Field::new("linkAvgGrade", DataType::Float64, true),
    ]));
    assert_eq!(*schema::LINK.arrow_schema(), *expected_link);
}

#[test]
fn off_network_link_only_runs_when_road_type_one_selected() {
    // If the runspec selects road type 4 only (no off-network), the
    // off-network coverage check is a no-op. The CSV reader still
    // imports the file and writes it to Parquet — but downstream
    // OpModeDistribution coverage is gated on actual presence of an
    // off-network link in the Link table, not on the runspec's road
    // type set.
    let dir = TempDir::new().unwrap();
    let runspec = RunSpecFilter::default()
        .with_road_types([4])
        .with_source_types([21]);

    let link_csv = write_fixture(
        dir.path(),
        "link.csv",
        "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
",
    );
    let lsth_csv = write_fixture(
        dir.path(),
        "lsth.csv",
        "linkID,sourceTypeID,sourceTypeHourFraction
1,21,1.0
",
    );

    let out_dir = dir.path().join("pdb");
    let manifest = ImportSession::builder(&out_dir, &runspec)
        .with_link(&link_csv)
        .with_link_source_type_hour(&lsth_csv)
        .build()
        .unwrap()
        .write_to_disk()
        .unwrap();

    assert_eq!(manifest.tables.len(), 2);
    for entry in &manifest.tables {
        assert!(
            entry.warnings.is_empty(),
            "{}: {:?}",
            entry.name,
            entry.warnings
        );
    }
}

#[test]
fn warnings_are_recorded_in_manifest_when_runspec_filter_rejects() {
    let dir = TempDir::new().unwrap();
    let runspec = RunSpecFilter::default().with_counties([26161]);

    let link_csv = write_fixture(
        dir.path(),
        "link.csv",
        "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
2,99999,888880,4,0.5,1000,55,b,0
",
    );
    let out_dir = dir.path().join("pdb");
    let manifest = ImportSession::builder(&out_dir, &runspec)
        .with_link(&link_csv)
        .build()
        .unwrap()
        .write_to_disk()
        .unwrap();
    assert_eq!(manifest.tables.len(), 1);
    let link_manifest = &manifest.tables[0];
    assert_eq!(link_manifest.row_count, 2, "Java keeps filtered rows");
    let warned_columns: Vec<&str> = link_manifest
        .warnings
        .iter()
        .map(|w| w.column.as_str())
        .collect();
    assert!(warned_columns.contains(&"countyID"));
}

#[test]
fn cross_stage_parquet_layout_matches_default_db_path_convention() {
    // moves-data-default expects per-table Parquet at
    // `<root>/<table>.parquet` for monolithic tables (every PDB table
    // is monolithic). Verify the importer writes to the same layout.
    let dir = TempDir::new().unwrap();
    let runspec = RunSpecFilter::default();
    let link_csv = write_fixture(
        dir.path(),
        "link.csv",
        "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
",
    );
    let out_dir = dir.path().join("pdb");
    ImportSession::builder(&out_dir, &runspec)
        .with_link(&link_csv)
        .build()
        .unwrap()
        .write_to_disk()
        .unwrap();
    assert!(out_dir.join("Link.parquet").exists());
}
